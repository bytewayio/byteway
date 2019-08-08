package cypress

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
)

const (
	stateUnlocked    = 0
	statePendingLock = 1
	stateLocked      = 2

	eventCreated = 0
	eventDeleted = 1
	eventExit    = 2
)

var (
	// ErrLockPending lock is pending
	ErrLockPending = errors.New("lock is pending")

	// ErrLockFailed failed to lock
	ErrLockFailed = errors.New("failed to lock")

	// ErrLockCancelled not able to acquire the lock before context has cancelled
	ErrLockCancelled = errors.New("context cancelled before lock is acquired")
)

// ZkConn zookeeper connection
type ZkConn struct {
	Conn   *zk.Conn
	EvChan <-chan zk.Event
}

// ZkLock zookeeper based distributed lock
type ZkLock struct {
	conn  *zk.Conn
	path  string
	state int32
}

// NewZkLock creates a new ZkLock on the given path
func NewZkLock(conn *zk.Conn, path string) *ZkLock {
	return &ZkLock{
		conn, path, stateUnlocked,
	}
}

// Lock lock or return error if not able to lock
func (lock *ZkLock) Lock(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&lock.state, stateUnlocked, statePendingLock) {
		return ErrLockPending
	}

	// ensure we reset pending lock to unlocked in case of lock failed
	defer atomic.CompareAndSwapInt32(&lock.state, statePendingLock, stateUnlocked)
	var cancelled int32
	cancelChannel := make(chan int32, 1)
	defer close(cancelChannel)
	go func() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded || ctx.Err() == context.Canceled {
				atomic.StoreInt32(&cancelled, 1)
				cancelChannel <- 1
			}
		case <-cancelChannel:
			break
		}
	}()
	for atomic.LoadInt32(&cancelled) == 0 {
		exists, _, ch, err := lock.conn.ExistsW(lock.path)
		if err != nil {
			zap.L().Error("failed to check node", zap.String("path", lock.path), zap.Error(err))
			return ErrLockFailed
		}

		if !exists {
			_, err = lock.conn.Create(lock.path, []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
			if err == nil {
				// wait and drop the create event
				event := <-ch
				if event.Type != zk.EventNodeCreated {
					zap.L().Error("unexpected event, NodeCreated is expected", zap.String("path", lock.path), zap.Int32("event", int32(event.Type)))
					return ErrLockFailed
				}

				if !atomic.CompareAndSwapInt32(&lock.state, statePendingLock, stateLocked) {
					zap.L().Error("unexpected lock state, pendingLock is expected", zap.String("path", lock.path), zap.Int32("state", lock.state))
					return ErrLockFailed
				}

				// exit for
				break
			} else if err == zk.ErrNodeExists {
				// lock has been placed by other process
				event := <-ch
				if event.Type != zk.EventNodeCreated {
					zap.L().Error("unexpected event type", zap.String("path", lock.path), zap.Int32("type", int32(event.Type)))
					return ErrLockFailed
				}
			} else {
				zap.L().Error("unexpected create error", zap.String("path", lock.path), zap.Error(err))
				return ErrLockFailed
			}
		} else {
			// wait for lock to be released or cancel event
			select {
			case event := <-ch:
				if event.Type != zk.EventNodeDeleted {
					zap.L().Error("unexpected event type", zap.String("path", lock.path), zap.Int32("type", int32(event.Type)))
					return ErrLockFailed
				}
				break
			case <-cancelChannel:
				break
			}
		}
	}

	if atomic.LoadInt32(&cancelled) == 1 {
		if atomic.LoadInt32(&lock.state) == stateLocked {
			lock.Release()
		}

		return ErrLockCancelled
	}

	return nil
}

// Release release the lock, ignore all errors
func (lock *ZkLock) Release() {
	err := lock.conn.Delete(lock.path, 0)
	if err != nil {
		zap.L().Error("failed to delete lock node", zap.String("path", lock.path), zap.Error(err))
	}

	if !atomic.CompareAndSwapInt32(&lock.state, stateLocked, stateUnlocked) {
		zap.L().Error("lock is not in locked state", zap.String("path", lock.path), zap.Int32("state", lock.state))
	}
}

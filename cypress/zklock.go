package cypress

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/zap"
)

const (
	eventCreated = 0
	eventDeleted = 1
	eventExit    = 2
)

var (
	// ErrLockFailed failed to lock
	ErrLockFailed = errors.New("failed to lock")

	// ErrLockCancelled not able to acquire the lock before context has cancelled
	ErrLockCancelled = errors.New("context cancelled before lock is acquired")
)

// ZkLock zookeeper based distributed lock
type ZkLock struct {
	conn      *zk.Conn
	path      string
	localLock *sync.Mutex
	acquired  bool
}

// NewZkLock creates a new ZkLock on the given path
func NewZkLock(conn *zk.Conn, path string) *ZkLock {
	return &ZkLock{
		conn, path, &sync.Mutex{}, false,
	}
}

// Lock lock or return error if not able to lock
func (lock *ZkLock) Lock(ctx context.Context) error {
	lock.localLock.Lock()
	unlockLocal := true
	defer func() {
		if unlockLocal {
			lock.localLock.Unlock()
		}
	}()

	if lock.acquired {
		panic("bad zklock state")
	}

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

				lock.acquired = true
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

	// if lock is acquired, local lock will be unlocked in Release
	unlockLocal = !lock.acquired
	if atomic.LoadInt32(&cancelled) == 1 {
		if lock.acquired {
			lock.Release()
		}

		return ErrLockCancelled
	}

	return nil
}

// Release release the lock, ignore all errors
func (lock *ZkLock) Release() {
	err := lock.conn.Delete(lock.path, -1)
	if err != nil {
		zap.L().Error("failed to delete lock node", zap.String("path", lock.path), zap.Error(err))
	}

	lock.acquired = false
	lock.localLock.Unlock()
}

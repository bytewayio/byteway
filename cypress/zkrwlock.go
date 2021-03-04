package cypress

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-zookeeper/zk"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	readersParentNode = "readers"
	writerNode        = "writer"
)

// ZkRWLock zookeeper based reader/writer lock
type ZkRWLock struct {
	conn               *zk.Conn
	path               string
	readerPath         string
	writerPath         string
	lock               *sync.Mutex
	readers            int32
	readerLockAcquired bool
	writerLockAcquired bool
	localLock          *sync.RWMutex
}

// NewZkRWLock creates a zookeeper reader/writer lock on the given path
func NewZkRWLock(conn *zk.Conn, path string) (*ZkRWLock, error) {
	_, err := conn.Create(path, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		return nil, err
	}

	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}

	_, err = conn.Create(path+readersParentNode, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		return nil, err
	}

	return &ZkRWLock{conn, path, path + readersParentNode + "/" + uuid.NewString(), path + writerNode, &sync.Mutex{}, 0, false, false, &sync.RWMutex{}}, nil
}

// RLock try to acquire reader lock
func (rwLock *ZkRWLock) RLock(ctx context.Context) error {
	rwLock.localLock.RLock()
	releaseLocalLock := false
	defer func() {
		if releaseLocalLock {
			rwLock.localLock.RUnlock()
		}
	}()

	rwLock.lock.Lock()
	defer rwLock.lock.Unlock()

	if rwLock.readerLockAcquired {
		atomic.AddInt32(&rwLock.readers, 1)
		return nil
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
		// 1. Create reader node
		_, err := rwLock.conn.Create(rwLock.readerPath, []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		if err != nil {
			releaseLocalLock = true
			if err == zk.ErrNodeExists {
				zap.L().Error("trying to acquire reader lock while it's acquired", zap.String("node", rwLock.readerPath), zap.String("trace", GetTraceID(ctx)), zap.Error(err))
			}

			return err
		}

		// 2. Check writer node
		exists, _, ch, err := rwLock.conn.ExistsW(rwLock.writerPath)
		if err != nil {
			releaseLocalLock = true
			zap.L().Error("failed to check writer node", zap.String("node", rwLock.writerPath), zap.String("trace", GetTraceID(ctx)), zap.Error(err))
			return err
		}

		// Node does not exist
		if !exists {
			rwLock.readerLockAcquired = true
			atomic.AddInt32(&rwLock.readers, 1)
			return nil
		}

		// Node exists
		// 3. Delete reader node
		err = rwLock.conn.Delete(rwLock.readerPath, -1)
		if err != nil {
			releaseLocalLock = true
			zap.L().Fatal("!!!failed to release reader node, writers will be hungry", zap.String("node", rwLock.readerPath), zap.Error(err), zap.String("trace", GetTraceID(ctx)))
			return err
		}

		select {
		case event := <-ch:
			if event.Type != zk.EventNodeDeleted {
				releaseLocalLock = true
				zap.L().Error("unexpected reader watch event", zap.Int32("event", int32(event.Type)), zap.String("reader", rwLock.readerPath), zap.String("trace", GetTraceID(ctx)))
				return ErrLockFailed
			}
			break
		case <-cancelChannel:
			break
		}
	}

	// Lock cancelled
	releaseLocalLock = true
	return ErrLockCancelled
}

// RUnlock release reader lock that is acquired
func (rwLock *ZkRWLock) RUnlock() {
	// release local lock
	rwLock.localLock.RUnlock()

	// if no more local readers, release zk reader node
	rwLock.lock.Lock()
	defer rwLock.lock.Unlock()
	if rwLock.readerLockAcquired && atomic.AddInt32(&rwLock.readers, -1) == 0 {
		rwLock.readerLockAcquired = false
		err := rwLock.conn.Delete(rwLock.readerPath, -1)
		if err != nil {
			zap.L().Fatal("!!!unable to delete reader node, writers will be hungry", zap.Error(err))
		}
	}
}

// Lock acquires writer lock
func (rwLock *ZkRWLock) Lock(ctx context.Context) error {
	rwLock.localLock.Lock()
	releaseLocalLock := false
	defer func() {
		if releaseLocalLock {
			rwLock.localLock.Unlock()
		}
	}()

	rwLock.lock.Lock()
	defer rwLock.lock.Unlock()

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
		// 1. Check writer node
		exists, _, ch, err := rwLock.conn.ExistsW(rwLock.writerPath)
		if err != nil {
			releaseLocalLock = true
			zap.L().Error("failed to check writer node exists", zap.String("node", rwLock.writerPath), zap.Error(err), zap.String("trace", GetTraceID(ctx)))
			return err
		}

		if exists {
			select {
			case event := <-ch:
				if event.Type != zk.EventNodeDeleted {
					releaseLocalLock = true
					zap.L().Error("unexpected node event", zap.String("node", rwLock.writerPath), zap.Int32("event", int32(event.Type)), zap.String("trace", GetTraceID(ctx)))
					return err
				}
				break
			case <-cancelChannel:
				releaseLocalLock = true
				return ErrLockCancelled
			}
		}

		// 2. Create writer node
		_, err = rwLock.conn.Create(rwLock.writerPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err == zk.ErrNodeExists {
			continue
		}

		if err != nil {
			releaseLocalLock = true
			zap.L().Error("failed to create writer node", zap.String("node", rwLock.writerPath), zap.Error(err), zap.String("trace", GetTraceID(ctx)))
			return err
		}

		// write node created
		// 3. Wait for readers if any
		for atomic.LoadInt32(&cancelled) == 0 {
			readers, _, readersCh, err := rwLock.conn.ChildrenW(rwLock.path + readersParentNode)
			if err != nil {
				releaseLocalLock = true
				zap.L().Error("failed to check readers", zap.String("node", rwLock.path+readersParentNode), zap.Error(err), zap.String("trace", GetTraceID(ctx)))
				return err
			}

			if len(readers) == 0 {
				// lock acquired
				rwLock.writerLockAcquired = true
				return nil
			}

			select {
			case event := <-readersCh:
				if event.Type != zk.EventNodeChildrenChanged {
					zap.L().Error("unexpected node event", zap.String("node", rwLock.writerPath), zap.Int32("event", int32(event.Type)), zap.String("trace", GetTraceID(ctx)))
				}
				break
			case <-cancelChannel:
				releaseLocalLock = true
				return ErrLockCancelled
			}
		}
	}

	// Lock cancelled
	releaseLocalLock = true
	return ErrLockCancelled
}

// Unlock release writer lock
func (rwLock *ZkRWLock) Unlock() {
	rwLock.localLock.Unlock()
	rwLock.lock.Unlock()
	defer rwLock.lock.Unlock()

	rwLock.writerLockAcquired = false
	err := rwLock.conn.Delete(rwLock.writerPath, -1)
	if err != nil {
		zap.L().Error("!!!failed to delete writer node, no more lock can be granted", zap.String("node", rwLock.writerPath), zap.Error(err))
	}
}

package cypress

import (
	"context"

	"github.com/go-zookeeper/zk"
	"go.uber.org/zap"
)

const (
	readerLockPrefix string = "reader-"
	writerLockPrefix string = "writer-"
)

type ZkRWLock2 struct {
	conn *zk.Conn
	path string
}

func NewZkRWLock2(conn *zk.Conn, path string) (*ZkRWLock2, error) {
	_, err := conn.Create(path, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		return nil, err
	}

	return &ZkRWLock2{
		conn: conn,
		path: path,
	}, nil
}

func (lock *ZkRWLock2) Lock(ctx context.Context) (LockContext, error) {
	return zkAcquireLock(ctx, lock.conn, lock.path, writerLockPrefix, writerLockPrefix, readerLockPrefix)
}

func (lock *ZkRWLock2) RLock(ctx context.Context) (LockContext, error) {
	return zkAcquireLock(ctx, lock.conn, lock.path, readerLockPrefix, writerLockPrefix, "")
}

func (lock *ZkRWLock2) Unlock(lockCtx LockContext) {
	if lockCtx == nil {
		zap.L().Error("unable to release lock node, no lock context")
		return
	}

	if v, ok := lockCtx[lockPathKey]; ok {
		if path, ok := v.(string); ok {
			if err := lock.conn.Delete(path, 0); err != nil {
				zap.L().Error("failed to release lock node", zap.Error(err), zap.String("path", path))
			}
		} else {
			zap.L().Error("unable to release lock node, bad lock node path")
		}
	} else {
		zap.L().Error("unable to release lock node, node path not found")
	}
}

func (lock *ZkRWLock2) RUnlock(lockCtx LockContext) {
	lock.Unlock(lockCtx)
}

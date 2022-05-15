package cypress

import (
	"context"
	"math"
	"strconv"

	"github.com/go-zookeeper/zk"
	"go.uber.org/zap"
)

var (
	ZkLock2MaxConcurrentThreads int64 = int64(math.MaxInt32) / 2
)

const (
	lockPrefix  string = "lock-"
	lockPathKey string = "lockPath"
)

// ZkLock zookeeper based distributed lock
type ZkLock2 struct {
	conn *zk.Conn
	path string
}

func findLeastSeqAndPrevNode(nodes []string, prefix string, target int64) (int64, string, error) {
	var minPositiveSeqNumber int64 = -1
	var prevSeqNumber int64 = math.MinInt64
	var maxSeqNumber int64 = math.MinInt64
	prevSeqNode := ""
	maxSeqNode := ""

	var minNegativeSeqNumber int64 = 0

	for _, v := range nodes {
		seq, err := strconv.ParseInt(v[len(lockPrefix):], 10, 64)
		if err != nil {
			zap.L().Error("unexpected lock sequential node", zap.Error(err), zap.String("node", v))
			return 0, "", err
		}

		if seq > maxSeqNumber {
			maxSeqNumber = seq
			maxSeqNode = v
		}

		if seq < target && seq > prevSeqNumber {
			prevSeqNumber = seq
			prevSeqNode = v
		}

		if seq >= 0 && (minPositiveSeqNumber < 0 || seq < minPositiveSeqNumber) {
			minPositiveSeqNumber = seq
		} else if seq < 0 && (minNegativeSeqNumber >= 0 || seq < minNegativeSeqNumber) {
			minNegativeSeqNumber = seq
		}
	}

	var minSeqNumber int64 = math.MinInt64
	if minNegativeSeqNumber >= 0 {
		minSeqNumber = minPositiveSeqNumber
	} else if minPositiveSeqNumber < 0 {
		minSeqNumber = minNegativeSeqNumber
	} else if minPositiveSeqNumber > (int64(math.MaxInt32) - ZkLock2MaxConcurrentThreads) {
		minSeqNumber = minPositiveSeqNumber
	} else {
		minSeqNumber = minNegativeSeqNumber
	}

	prevNode := ""
	if minSeqNumber != target {
		if prevSeqNumber > math.MinInt64 {
			prevNode = prevSeqNode
		} else {
			prevNode = maxSeqNode
		}
	}

	return minSeqNumber, prevNode, nil
}

// NewZkLock creates a new ZkLock2 on the given path
func NewZkLock2(conn *zk.Conn, path string) (*ZkLock2, error) {
	if _, err := conn.Create(path, []byte{}, 0, zk.WorldACL(zk.PermAll)); err != nil && err != zk.ErrNodeExists {
		return nil, err
	}

	return &ZkLock2{
		conn, path,
	}, nil
}

func (lock *ZkLock2) Lock(ctx context.Context) (LockContext, error) {
	cancelled := false
	doneChan := make(chan bool, 1)
	defer close(doneChan)
	go func() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded || ctx.Err() == context.Canceled {
				cancelled = true
				doneChan <- true
			}
		case <-doneChan:
			break
		}
	}()

	// Lamport's Bakery algorithm
	// Set flag and get a number that is greater than any other signed numbers
	// which is implemented in zookeeper as create a sequential node under a
	// lock container
	lockPath, err := lock.conn.Create(lock.path+"/"+lockPrefix, []byte{}, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		zap.L().Error("failed to create lock sequential node", zap.String("path", lock.path+"/"+lockPrefix), zap.Error(err))
		doneChan <- true
		return nil, err
	}

	lockAcquired := false
	defer func() {
		if !lockAcquired {
			// exit but lock is not acquired, delete the node that has been created
			if err := lock.conn.Delete(lockPath, 0); err != nil {
				zap.L().Error("failed to delete lock sequential node", zap.Error(err), zap.String("path", lockPath))
			}
		}
	}()

	seqNumber, err := strconv.ParseInt(lockPath[len(lock.path)+len(lockPrefix)+2:], 10, 64)
	if err != nil {
		zap.L().Error("failed to parse sequence number", zap.String("path", lockPath), zap.Error(err))
		doneChan <- true
		return nil, err
	}

	for !cancelled {
		children, _, err := lock.conn.Children(lock.path)
		if err != nil {
			zap.L().Error("failed to list all lock sequential nodes", zap.String("path", lock.path), zap.Error(err))
			doneChan <- true
			return nil, err
		}

		minSeqNumber, prevNode, err := findLeastSeqAndPrevNode(children, lockPrefix, seqNumber)
		if err != nil {
			zap.L().Error("bad sequential node detected", zap.Error(err), zap.String("path", lock.path))
			doneChan <- true
			return nil, err
		}

		if seqNumber == minSeqNumber {
			lockAcquired = true
			doneChan <- true
			return map[string]interface{}{
				lockPathKey: lockPath,
			}, nil
		}

		exists, _, ch, err := lock.conn.ExistsW(lock.path + "/" + prevNode)
		if err != nil {
			zap.L().Error("failed to watch on lock node", zap.Error(err), zap.String("node", prevNode))
			doneChan <- true
			return nil, err
		}

		if exists {
			<-ch
		}
	}

	return nil, ErrLockCancelled
}

func (lock *ZkLock2) Unlock(lockCtx LockContext) {
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

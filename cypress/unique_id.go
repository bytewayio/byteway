package cypress

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

const (
	// PartitionKeyBitWidth bits for partition value
	PartitionKeyBitWidth = 5

	// PartitionKeyMask partition value mask
	PartitionKeyMask = 0x1f

	// SegmentedIDBitWidth segmented id value bit width
	SegmentedIDBitWidth = 7

	// SegmentedIDMask segmented id value mask
	SegmentedIDMask = 0x7f

	// PooledIDBitWidth pooled id value bit width
	PooledIDBitWidth = PartitionKeyBitWidth + SegmentedIDBitWidth

	// MaxPooledID maximum pooled id
	MaxPooledID = (int64(1) << (64 - PooledIDBitWidth)) - 1

	// MaxSegmentedID max segmented id
	MaxSegmentedID = int64(1 << SegmentedIDBitWidth)
)

var (
	// ErrOutOfRange value out of range
	ErrOutOfRange = errors.New("value out of range")
)

// PartitionCalculator partition key calculator
type PartitionCalculator interface {
	GetPartition(key string) int32
}

// UniqueID global unique id for a given namespace in cluster
type UniqueID struct {
	Value int64 // format [PooledID][Partition][SegmentedID]
}

// Partition gets the partition for the unique id
func (id UniqueID) Partition() int32 {
	return GetPartitionKey(id.Value)
}

// NewUniqueID create a new unique id based on the pooledID, partition and segmentedID
func NewUniqueID(pooledID int64, partition int32, segmentedID int32) (UniqueID, error) {
	if pooledID > MaxPooledID || partition > PartitionKeyMask || segmentedID > SegmentedIDMask {
		return UniqueID{0}, ErrOutOfRange
	}

	return UniqueID{(pooledID << PooledIDBitWidth) | int64((partition<<SegmentedIDBitWidth)|segmentedID)}, nil
}

// GetPartitionKey gets the partition key from the given unique ID value
func GetPartitionKey(id int64) int32 {
	return int32((id >> SegmentedIDBitWidth) & PartitionKeyMask)
}

// PartitionCalculateFunc partition calculate function, implements PartitionCalculator
type PartitionCalculateFunc func(key string) int32

// GetPartition implements PartitionCalculator
func (calc PartitionCalculateFunc) GetPartition(key string) int32 {
	return calc(key)
}

// CalculateMd5PartitionKey Md5 based partition key calculator
func CalculateMd5PartitionKey(key string) int32 {
	data := Md5([]byte(key))
	id := binary.BigEndian.Uint64(data[0:8])
	return int32(id % PartitionKeyMask)
}

// UniqueIDPool unique id pool
type UniqueIDPool struct {
	pooledIDs      []int64
	weightedQueues []*semaphore.Weighted
	Lock           *sync.Mutex
}

// NewUniqueIDPool creates a new ID pool
func NewUniqueIDPool() *UniqueIDPool {
	pooledIDs := make([]int64, PartitionKeyMask+1)
	weightedQueues := make([]*semaphore.Weighted, 1<<PartitionKeyBitWidth)
	for i := 0; i < len(pooledIDs); i = i + 1 {
		pooledIDs[i] = MaxSegmentedID
	}

	for i := 0; i < len(weightedQueues); i = i + 1 {
		weightedQueues[i] = semaphore.NewWeighted(SegmentedIDMask)
	}

	return &UniqueIDPool{
		pooledIDs:      pooledIDs,
		weightedQueues: weightedQueues,
		Lock:           &sync.Mutex{},
	}
}

// NextID generate a new id from pool
func (pool *UniqueIDPool) NextID(ctx context.Context, partition int32) (int64, error) {
	if partition > PartitionKeyMask {
		return 0, ErrOutOfRange
	}

	err := pool.weightedQueues[partition].Acquire(ctx, 1)
	if err != nil {
		return 0, err
	}

	defer pool.weightedQueues[partition].Release(1)
	return atomic.AddInt64(&pool.pooledIDs[partition], 1), nil
}

// UpdatePooledID update pooled id for the given partition
func (pool *UniqueIDPool) UpdatePooledID(partition int32, pooledID int64) error {
	if partition > PartitionKeyMask {
		return ErrOutOfRange
	}

	atomic.StoreInt64(&pool.pooledIDs[partition], pooledID<<PooledIDBitWidth)
	return nil
}

// UniqueIDGenerator unique id generator interface
type UniqueIDGenerator interface {
	NextUniqueID(ctx context.Context, name string, partition int32) (UniqueID, error)
}

// PooledID unique id pooled ID record, requires the following table
/// create table `pooled_id` (
/// `name` varchar(200) not null,
/// `partition` int not null,
/// `pooled_id` bigint not null,
/// constraint `pk_id_generator` primary key (`name`, `partition`)
/// );
type PooledID struct {
	Name      string `col:"name" dtags:"key,nogen"`
	Partition int32  `col:"partition"`
	PooledID  int64  `col:"pooled_id"`
}

var pooledIDPrototype = &PooledID{}

// DbUniqueIDGenerator database based unique id generator
type DbUniqueIDGenerator struct {
	dbAccessor *DbAccessor
	pools      *ConcurrentMap
}

// implement UniqueIDGenerator

func isValidID(id int64) bool {
	return (id & int64(PartitionKeyMask<<SegmentedIDBitWidth)) == 0
}

// NewDbUniqueIDGenerator creates a db based unique id generator
func NewDbUniqueIDGenerator(dbAccessor *DbAccessor) *DbUniqueIDGenerator {
	return &DbUniqueIDGenerator{
		dbAccessor: dbAccessor,
		pools:      NewConcurrentMapTypeEnforced(reflect.TypeOf(&UniqueIDPool{})),
	}
}

type insertError struct {
	err error
}

func (e *insertError) Error() string {
	return e.err.Error()
}

func (e *insertError) Unwrap() error {
	return e.err
}

// NextUniqueID generate a next unique id
func (generator *DbUniqueIDGenerator) NextUniqueID(ctx context.Context, name string, partition int32) (UniqueID, error) {
	var uniqueID UniqueID
	err := LogOperation(ctx, "GenerateUniqueId", func() error {
		pool := generator.pools.GetOrCompute(name, func() interface{} {
			return NewUniqueIDPool()
		}).(*UniqueIDPool)
		id, err := pool.NextID(ctx, partition)
		if err != nil {
			return err
		}

		for !isValidID(id) {
			retryLeft := 3
			for retryLeft > 0 {
				err = func() error {
					pool.Lock.Lock()
					defer pool.Lock.Unlock()
					id, err = pool.NextID(ctx, partition)
					if isValidID(id) {
						return nil
					}

					txn, txnErr := generator.dbAccessor.BeginTxn(ctx)
					if txnErr != nil {
						return txnErr
					}

					defer txn.Close()

					obj, queryErr := txn.QueryOne(
						"select * from `pooled_id` where `name`=? and `partition`=? for update",
						NewSmartMapper(pooledIDPrototype),
						name, partition)
					if queryErr != nil {
						return queryErr
					}

					if obj == nil {
						_, err = txn.Insert(&PooledID{
							Name:      name,
							Partition: partition,
							PooledID:  1,
						})
						if err != nil {
							return &insertError{err}
						}

						pool.UpdatePooledID(partition, 1)
					} else {
						pooledID := obj.(*PooledID)
						pooledID.PooledID++
						_, err = txn.Execute(
							"update `pooled_id` set `pooled_id`=? where `name`=? and `partition`=?",
							pooledID.PooledID,
							name,
							partition)
						if err != nil {
							return err
						}

						pool.UpdatePooledID(partition, pooledID.PooledID)
					}

					id, err = pool.NextID(ctx, partition)
					return err
				}()

				if err != nil {
					if e, ok := err.(*insertError); ok {
						retryLeft--
						if retryLeft == 0 {
							err = e.Unwrap()
						} else {
							zap.L().Error("failed to allocate unique id", zap.Error(e.Unwrap()), zap.Int("retry", retryLeft))
							continue
						}
					}
				}

				// no need to retry
				break
			}

			if err != nil {
				return err
			}
		}

		uniqueID, err = NewUniqueID(id>>PooledIDBitWidth, partition, int32(id&SegmentedIDMask))
		if err != nil {
			return err
		}

		return nil
	})

	return uniqueID, err
}
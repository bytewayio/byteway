package cypress

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

const (
	// ClusterTxnStateNone nil state
	ClusterTxnStateNone = iota

	// ClusterTxnStateCommitted state committed
	ClusterTxnStateCommitted

	// ClusterTxnStateRollback state rollback
	ClusterTxnStateRollback
)

const (
	// XAStateUnknown state unknown
	XAStateUnknown = iota

	// XAStateActive state active
	XAStateActive

	// XAStateIdle state idle
	XAStateIdle

	// XAStatePrepared state prepared
	XAStatePrepared

	// XAStateRollback state rollback
	XAStateRollback

	// XAStateCommitted state committed
	XAStateCommitted
)

// ClusterTxn cluster transaction entity
type ClusterTxn struct {
	ID              string `col:"id" dtags:"key,nogen"`
	State           int    `col:"state"`
	Timestamp       int64  `col:"timestamp"`
	LeaseExpiration int64  `col:"lease_expiration"`
}

// TxnParticipant cluster transaction participant
type TxnParticipant struct {
	TxnID     string `col:"txn_id" dtags:"multikey"`
	Partition int    `col:"partition" dtags:"multikey"`
	State     int    `col:"state"`
}

// XaTxnID xa transaction id format
type XaTxnID struct {
	FormatID       int    `col:"formatID"`
	GlobalIDLength int    `col:"gtrid_length"`
	BranchIDLength int    `col:"bqual_length"`
	Data           string `col:"data"`
}

// TxnStore a cluster transaction entity store to keep cluster transaction states
type TxnStore interface {
	// CreateTxn create a transaction entity
	CreateTxn(ctx context.Context, txnID string, t time.Time) (*ClusterTxn, error)

	// GetTxn get transaction entity by ID
	GetTxn(ctx context.Context, txnID string) (*ClusterTxn, error)

	// UpdateTxnState update transaction state
	UpdateTxnState(ctx context.Context, txnID string, state int) error

	// AddTxnParticipant add a participant to a transaction
	AddTxnParticipant(ctx context.Context, txnID string, partition int) error

	// DeleteTxn delete partition from store
	DeleteTxn(ctx context.Context, txnID string) error

	// LeaseExpiredTxns lease expired transactions, only expired and lease expired (or not lease) transactions are visible
	LeaseExpiredTxns(ctx context.Context, expireTime, leaseTimeout time.Time, items int) ([]*ClusterTxn, error)

	// ListParticipants list all participants in transaction
	ListParticipants(ctx context.Context, txnID string) ([]*TxnParticipant, error)

	// Close close all resources used by store
	Close() error
}

type unknownStateTxnResolver struct {
	txnStore   TxnStore
	partitions []*DbAccessor
	txnTimeout int
}

func (r *unknownStateTxnResolver) resolve(txnID string) {
	ids := strings.Split(txnID, ",")
	for i := 0; i < len(ids); i = i + 1 {
		ids[i] = strings.Trim(ids[i], "'")
	}

	if len(ids) != 2 {
		zap.L().Warn("invalid transaction id format", zap.String("txnId", txnID))
		return
	}

	r.resolveTxn(ids[0])
}

func (r *unknownStateTxnResolver) resolveTxn(txnID string) {
	ctx := context.Background()
	txn, err := r.txnStore.GetTxn(ctx, txnID)
	if err != nil {
		zap.L().Error("failed to get cluster txn entity", zap.Error(err))
		return
	}

	if txn == nil {
		zap.L().Error("cluster txn entity not found", zap.String("id", txnID))
		return
	}

	action := "ROLLBACK"
	if txn.State == ClusterTxnStateCommitted {
		action = "COMMIT"
	}

	participants, err := r.txnStore.ListParticipants(ctx, txn.ID)
	if err != nil {
		zap.L().Error("failed to query participants", zap.Error(err))
		return
	}

	for _, p := range participants {
		participant := p.Partition
		if participant < 0 {
			zap.L().Info("invalid participant for transaction", zap.Int("participant", participant), zap.String("txnID", txnID))
			continue
		}

		data := fmt.Sprintf("%v%v", txnID, participant)
		xaid := fmt.Sprintf("'%v','%v'", txnID, participant)
		txnFound := false
		dbAccessor := r.partitions[participant%len(r.partitions)]
		xaidList, err := dbAccessor.QueryAll(context.Background(), "XA RECOVER", NewSmartMapper(&XaTxnID{}))
		if err != nil {
			zap.L().Error("failed to query xaid", zap.Error(err), zap.Int("participant", participant))
			return
		}

		for _, item := range xaidList {
			if item.(*XaTxnID).Data == data {
				txnFound = true
				break
			}
		}

		if txnFound {
			_, err = dbAccessor.Execute(context.Background(), fmt.Sprintf("XA %v %v", action, xaid))
			if err != nil {
				zap.L().Error("failed to fix transaction", zap.Error(err), zap.String("txnID", txnID), zap.Int("participant", participant))
				return
			}

			zap.L().Info("transaction fixed", zap.String("txnID", txnID), zap.Int("participant", participant))
		}
	}

	err = r.txnStore.DeleteTxn(ctx, txnID)
	if err != nil {
		zap.L().Info("failed to delete transaction entity", zap.String("txnID", txnID))
	}
}

func (r *unknownStateTxnResolver) startMonitor(done <-chan bool) {
	ticker := time.NewTicker(time.Duration(r.txnTimeout/2) * time.Second)
	defer ticker.Stop()
	for {
		leaseTimeout := time.Now().Add(time.Minute)
		txnExpiration := time.Now().Add(time.Second * time.Duration(-r.txnTimeout))
		expiredTxns, err := r.txnStore.LeaseExpiredTxns(context.Background(), txnExpiration, leaseTimeout, 20)
		if err != nil {
			zap.L().Error("failed to lease expired transactions for processing", zap.Error(err))
		} else {
			for _, txn := range expiredTxns {
				r.resolveTxn(txn.ID)
			}
		}

		if done == nil {
			<-ticker.C
		} else {
			select {
			case <-ticker.C:
				break
			case <-done:
				return
			}
		}
	}
}

// XATransaction MySQL XA transaction support
type XATransaction struct {
	ctx      context.Context
	state    int
	conn     *sql.Conn
	resolver *unknownStateTxnResolver
	id       string
}

func newXATransaction(ctx context.Context, id string, conn *sql.Conn, resolver *unknownStateTxnResolver) *XATransaction {
	return &XATransaction{
		ctx:      ctx,
		state:    XAStateUnknown,
		conn:     conn,
		resolver: resolver,
		id:       id,
	}
}

func (xa *XATransaction) execXACommand(command string) error {
	return LogOperation(xa.ctx, "ExecXACommand", func() error {
		_, err := xa.conn.ExecContext(xa.ctx, command)
		return err
	})
}

// Begin begins the transaction
func (xa *XATransaction) Begin() error {
	err := xa.execXACommand("XA START " + xa.id)
	if err == nil {
		xa.state = XAStateActive
	}

	return err
}

func (xa *XATransaction) prepare() error {
	if xa.state == XAStateUnknown {
		return nil // nothing to commit
	}

	if xa.state != XAStateActive {
		return errors.New("prepare is not allowed in this state" + strconv.Itoa(xa.state))
	}

	err := xa.execXACommand("XA END " + xa.id)
	if err != nil {
		return err
	}

	xa.state = XAStateIdle
	err = xa.execXACommand("XA PREPARE " + xa.id)
	if err != nil {
		return err
	}

	xa.state = XAStatePrepared
	return nil
}

func (xa *XATransaction) commit() error {
	if xa.state == XAStateUnknown {
		return nil // nothing to commit
	}

	if xa.state != XAStatePrepared {
		return errors.New("commit is not allowed in this state " + strconv.Itoa(xa.state))
	}

	err := xa.execXACommand("XA COMMIT " + xa.id)
	if err == nil {
		xa.state = XAStateCommitted
	}

	return err
}

func (xa *XATransaction) rollback() error {
	if xa.state == XAStateUnknown {
		return nil // nothing to do
	}

	if xa.state == XAStateActive {
		err := xa.execXACommand("XA END " + xa.id)
		if err != nil {
			return err
		}

		xa.state = XAStateIdle
	}

	if xa.state != XAStatePrepared && xa.state != XAStateIdle {
		return errors.New("rollback is not allowed in this state " + strconv.Itoa(xa.state))
	}

	err := xa.execXACommand("XA ROLLBACK " + xa.id)
	if err == nil {
		xa.state = XAStateRollback
	}

	return err
}

// Close release xa transaction
func (xa *XATransaction) Close() {
	if xa.state == XAStatePrepared {
		xa.resolver.resolve(xa.id)
	} else if xa.state == XAStateActive || xa.state == XAStateIdle {
		if err := xa.rollback(); err != nil {
			zap.L().Error("failed to rollback faulted transaction", zap.Error(err), zap.String("txnID", xa.id))
		}
	}

	xa.conn.Close()
}

// Insert insert entity to db
func (xa *XATransaction) Insert(entity interface{}) (sql.Result, error) {
	ty := reflect.TypeOf(entity)
	var r sql.Result
	var err error
	if ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	LogOperation(xa.ctx, "Insert"+ty.Name(), func() error {
		if xa.state == XAStateUnknown {
			e := xa.Begin()
			if e != nil {
				return e
			}
		}

		ed := GetOrCreateEntityDescriptor(reflect.TypeOf(entity))
		r, err = xa.conn.ExecContext(xa.ctx, ed.InsertSQL, ed.GetInsertValues(entity)...)
		return err
	})

	return r, err
}

// Update update entity with db
func (xa *XATransaction) Update(entity interface{}) (sql.Result, error) {
	ty := reflect.TypeOf(entity)
	var r sql.Result
	var err error
	if ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	LogOperation(xa.ctx, "Update"+ty.Name(), func() error {
		if xa.state == XAStateUnknown {
			e := xa.Begin()
			if e != nil {
				return e
			}
		}

		ed := GetOrCreateEntityDescriptor(reflect.TypeOf(entity))
		args := ed.GetUpdateValues(entity)
		args = append(args, ed.GetKeyValues(entity)...)
		r, err = xa.conn.ExecContext(xa.ctx, ed.UpdateSQL, args...)
		return err
	})

	return r, err
}

// Delete delete entity from db
func (xa *XATransaction) Delete(entity interface{}) (sql.Result, error) {
	ty := reflect.TypeOf(entity)
	var r sql.Result
	var err error
	if ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	LogOperation(xa.ctx, "Delete"+ty.Name(), func() error {
		if xa.state == XAStateUnknown {
			e := xa.Begin()
			if e != nil {
				return e
			}
		}

		ed := GetOrCreateEntityDescriptor(reflect.TypeOf(entity))
		r, err = xa.conn.ExecContext(xa.ctx, ed.DeleteSQL, ed.GetKeyValues(entity)...)
		return err
	})

	return r, err
}

// Execute execute command towards db
func (xa *XATransaction) Execute(command string, args ...interface{}) (sql.Result, error) {
	var r sql.Result
	var err error
	LogOperation(xa.ctx, "ExecuteCommand", func() error {
		if xa.state == XAStateUnknown {
			e := xa.Begin()
			if e != nil {
				return e
			}
		}

		r, err = xa.conn.ExecContext(xa.ctx, command, args...)
		return err
	})

	return r, err
}

// GetOneByKey query one entity based on the prototype and the single dimension key
func (xa *XATransaction) GetOneByKey(proto interface{}, key interface{}) (interface{}, error) {
	ty := reflect.TypeOf(proto)
	mapper := NewSmartMapper(proto)
	var result interface{}
	var err error
	if ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	LogOperation(xa.ctx, "GetOneByKey"+ty.Name(), func() error {
		if xa.state == XAStateUnknown {
			e := xa.Begin()
			if e != nil {
				return e
			}
		}

		ed := GetOrCreateEntityDescriptor(ty)
		result, err = QueryOne(xa.ctx, xa.conn, mapper, ed.GetOneSQL, key)
		return err
	})

	return result, err
}

// GetOne query one entity based on the keys in prototype
func (xa *XATransaction) GetOne(proto interface{}) (interface{}, error) {
	ty := reflect.TypeOf(proto)
	mapper := NewSmartMapper(proto)
	var result interface{}
	var err error
	if ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	LogOperation(xa.ctx, "GetOne"+ty.Name(), func() error {
		if xa.state == XAStateUnknown {
			e := xa.Begin()
			if e != nil {
				return e
			}
		}

		ed := GetOrCreateEntityDescriptor(ty)
		result, err = QueryOne(xa.ctx, xa.conn, mapper, ed.GetOneSQL, ed.GetKeyValues(proto)...)
		return err
	})

	return result, err
}

// QueryOne execute sql query and return one entity based on the mapper
func (xa *XATransaction) QueryOne(query string, mapper RowMapper, args ...interface{}) (interface{}, error) {
	var result interface{}
	var err error
	LogOperation(xa.ctx, "QueryOne", func() error {
		if xa.state == XAStateUnknown {
			e := xa.Begin()
			if e != nil {
				return e
			}
		}

		result, err = QueryOne(xa.ctx, xa.conn, mapper, query, args...)
		return err
	})

	return result, err
}

// QueryAll execute sql query and return all entities based on the mapper
func (xa *XATransaction) QueryAll(query string, mapper RowMapper, args ...interface{}) ([]interface{}, error) {
	var result []interface{}
	var err error
	LogOperation(xa.ctx, "QueryAll", func() error {
		if xa.state == XAStateUnknown {
			e := xa.Begin()
			if e != nil {
				return e
			}
		}

		result, err = QueryAll(xa.ctx, xa.conn, mapper, query, args...)
		return err
	})

	return result, err
}

// MyClusterTxn cluster supported 2PC transaction
type MyClusterTxn struct {
	id            string
	ctx           context.Context
	txnStore      TxnStore
	partitions    []*DbAccessor
	participants  map[int]*XATransaction
	idGen         UniqueIDGenerator
	resolver      *unknownStateTxnResolver
	partitionCalc PartitionCalculator
}

func newMyClusterTxn(
	ctx context.Context,
	id string,
	txnStore TxnStore,
	partitions []*DbAccessor,
	idGen UniqueIDGenerator,
	partitionCalc PartitionCalculator,
	resolver *unknownStateTxnResolver) *MyClusterTxn {
	return &MyClusterTxn{
		id:            id,
		ctx:           ctx,
		txnStore:      txnStore,
		partitions:    partitions,
		participants:  make(map[int]*XATransaction),
		idGen:         idGen,
		resolver:      resolver,
		partitionCalc: partitionCalc,
	}
}

// Commit commit all participants
func (txn *MyClusterTxn) Commit() error {
	for _, t := range txn.participants {
		err := t.prepare()
		if err != nil {
			return err
		}
	}

	// mark for resolver in case of failure
	err := txn.txnStore.UpdateTxnState(txn.ctx, txn.id, ClusterTxnStateCommitted)
	if err != nil {
		return err
	}

	// commit all participants
	for _, t := range txn.participants {
		err := t.commit()
		if err != nil {
			return err
		}
	}

	// cleanup all coordinator related data
	err = txn.txnStore.DeleteTxn(txn.ctx, txn.id)
	if err != nil {
		return err
	}

	return nil
}

// Rollback rollback all participants
func (txn *MyClusterTxn) Rollback() error {
	for _, t := range txn.participants {
		err := t.rollback()
		if err != nil {
			return err
		}
	}

	// cleanup all coordinator related data
	err := txn.txnStore.DeleteTxn(txn.ctx, txn.id)
	if err != nil {
		return err
	}

	return nil
}

// GetTxnByPartition get an XATransaction by partition number
func (txn *MyClusterTxn) GetTxnByPartition(partition int32) (*XATransaction, error) {
	physicalPartition := int(partition) % len(txn.partitions)
	t, ok := txn.participants[physicalPartition]
	if ok {
		return t, nil
	}

	err := txn.txnStore.AddTxnParticipant(txn.ctx, txn.id, physicalPartition)
	if err != nil {
		return nil, err
	}

	conn, err := txn.partitions[physicalPartition].Conn(txn.ctx)
	if err != nil {
		return nil, err
	}

	txn.participants[physicalPartition] = newXATransaction(txn.ctx, fmt.Sprintf("'%v','%v'", txn.id, physicalPartition), conn, txn.resolver)
	return txn.participants[physicalPartition], nil
}

// GetTxnByID get an XATransaction by unique ID
func (txn *MyClusterTxn) GetTxnByID(id int64) (*XATransaction, error) {
	return txn.GetTxnByPartition(GetPartitionKey(id))
}

// Close close and release all transactions
func (txn *MyClusterTxn) Close() {
	for _, t := range txn.participants {
		t.Close()
	}
}

// InsertAt insert an entity to the given partition
func (txn *MyClusterTxn) InsertAt(partition int32, entity interface{}) (sql.Result, error) {
	descriptor := GetOrCreateEntityDescriptor(reflect.TypeOf(entity))
	if descriptor.key != nil && descriptor.key.autoGen {
		id, err := txn.idGen.NextUniqueID(txn.ctx, descriptor.tableName, partition)
		if err != nil {
			return nil, err
		}

		v := reflect.ValueOf(entity)
		for v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		f := v.FieldByIndex(descriptor.key.field.field.Index)
		f.SetInt(id.Value)
	}

	xa, err := txn.GetTxnByPartition(partition)
	if err != nil {
		return nil, err
	}

	return xa.Insert(entity)
}

// InsertToAll insert an entry to all partitions
func (txn *MyClusterTxn) InsertToAll(entity interface{}) ([]sql.Result, error) {
	results := make([]sql.Result, len(txn.partitions))
	var err error
	for i := range txn.partitions {
		results[i], err = txn.InsertAt(int32(i), entity)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// Update update entity to database
func (txn *MyClusterTxn) Update(entity interface{}) (sql.Result, error) {
	descriptor := GetOrCreateEntityDescriptor(reflect.TypeOf(entity))
	v := reflect.ValueOf(entity)
	partition, err := txn.getPartitionFromEntity(descriptor, &v)
	if err != nil {
		return nil, err
	}

	xa, err := txn.GetTxnByPartition(partition)
	if err != nil {
		return nil, err
	}

	return xa.Update(entity)
}

// UpdateAt update entity at the specific partition
func (txn *MyClusterTxn) UpdateAt(partition int32, entity interface{}) (sql.Result, error) {
	xa, err := txn.GetTxnByPartition(partition)
	if err != nil {
		return nil, err
	}

	return xa.Update(entity)
}

// UpdateToAll update entity to all partitions
func (txn *MyClusterTxn) UpdateToAll(entity interface{}) ([]sql.Result, error) {
	results := make([]sql.Result, len(txn.partitions))
	var err error
	for i := range txn.partitions {
		results[i], err = txn.UpdateAt(int32(i), entity)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// Delete delete the entity from database
func (txn *MyClusterTxn) Delete(entity interface{}) (sql.Result, error) {
	descriptor := GetOrCreateEntityDescriptor(reflect.TypeOf(entity))
	v := reflect.ValueOf(entity)
	partition, err := txn.getPartitionFromEntity(descriptor, &v)
	if err != nil {
		return nil, err
	}

	xa, err := txn.GetTxnByPartition(partition)
	if err != nil {
		return nil, err
	}

	return xa.Delete(entity)
}

// DeleteAt delete the specified entry from the given partition
func (txn *MyClusterTxn) DeleteAt(partition int32, entity interface{}) (sql.Result, error) {
	xa, err := txn.GetTxnByPartition(partition)
	if err != nil {
		return nil, err
	}

	return xa.Delete(entity)
}

// DeleteFromAll delete the entry from all partitions
func (txn *MyClusterTxn) DeleteFromAll(entity interface{}) ([]sql.Result, error) {
	results := make([]sql.Result, len(txn.partitions))
	var err error
	for i := range txn.partitions {
		results[i], err = txn.DeleteAt(int32(i), entity)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// ExecuteAt execute the given query on a specific partition
func (txn *MyClusterTxn) ExecuteAt(partition int32, sql string, args ...interface{}) (sql.Result, error) {
	tx, err := txn.GetTxnByPartition(partition)
	if err != nil {
		return nil, err
	}

	return tx.Execute(sql, args...)
}

// ExecuteOnAll execute the given query on all partitions
func (txn *MyClusterTxn) ExecuteOnAll(query string, args ...interface{}) ([]sql.Result, error) {
	var err error
	results := make([]sql.Result, len(txn.partitions))
	for i := range txn.partitions {
		results[i], err = txn.ExecuteAt(int32(i), query, args...)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

func (txn *MyClusterTxn) getPartitionFromEntity(descriptor *EntityDescriptor, entityValue *reflect.Value) (int32, error) {
	if entityValue.Kind() == reflect.Ptr {
		v := entityValue.Elem()
		entityValue = &v
	}

	if descriptor.key == nil {
		if descriptor.partitionKey == nil {
			return -1, errors.New("No partition key or key defined")
		}

		partitionKey := entityValue.FieldByIndex(descriptor.partitionKey.field.Index).Interface()
		strValue, ok := partitionKey.(string)

		var partition int32 = -1
		if ok {
			partition = txn.partitionCalc.GetPartition(strValue)
		} else if intValue, ok := partitionKey.(int64); ok {
			partition = GetPartitionKey(intValue)
		}

		if partition == -1 {
			return -1, errors.New("Not able to get partition for entity")
		}

		return partition, nil
	}

	id, ok := entityValue.FieldByIndex(descriptor.key.field.field.Index).Interface().(int64)
	if !ok {
		return -1, errors.New("invalid key value")
	}

	return GetPartitionKey(id), nil
}

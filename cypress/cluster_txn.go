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

// ClusterTxn cluster transaction object in master database
type ClusterTxn struct {
	ID        string `col:"id" dtags:"key,nogen"`
	State     int    `col:"state"`
	Timestamp int64  `col:"timestamp"`
}

// XaTxnID xa transaction id format
type XaTxnID struct {
	FormatID       int    `col:"formatID"`
	GlobalIDLength int    `col:"gtrid_length"`
	BranchIDLength int    `col:"bqual_length"`
	Data           string `col:"data"`
}

type unknownStateTxnResolver struct {
	master     *DbAccessor
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
	dbTxn, err := r.master.BeginTxn(context.Background())
	if err != nil {
		zap.L().Error("failed to start transaction to resolve unknown state txn", zap.Error(err))
		return
	}

	defer dbTxn.Close()
	item, err := dbTxn.QueryOne("select * from `cluster_txn` where `id`=? for update", NewSmartMapper(&ClusterTxn{}), txnID)
	if err != nil {
		zap.L().Error("failed to get transaction", zap.String("txnID", txnID))
		return
	}

	txn := item.(*ClusterTxn)
	action := "ROLLBACK"
	if txn.State == ClusterTxnStateCommitted {
		action = "COMMIT"
	}

	participants, err := dbTxn.QueryAll("select `partition` from `txn_participant` where `txn_id`=?", NewSmartMapper(0), txnID)
	if err != nil {
		zap.L().Error("failed to query participants", zap.Error(err))
		return
	}

	for _, p := range participants {
		participant := p.(int)
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
			} else {
				zap.L().Info("transaction fixed", zap.String("txnID", txnID), zap.Int("participant", participant))
			}
		}
	}

	_, err = dbTxn.Execute("delete from `cluster_txn` where `id`=?", txnID)
	if err != nil {
		zap.L().Info("failed to cleanup txn from master", zap.String("txnID", txnID))
	}

	_, err = dbTxn.Execute("delete from `txn_participant` where `txn_id`=?", txnID)
	if err != nil {
		zap.L().Info("failed to cleanup participants from master", zap.String("txnID", txnID))
	}
}

func (r *unknownStateTxnResolver) startMonitor(done <-chan bool) {
	ticker := time.NewTicker(time.Duration(r.txnTimeout/2) * time.Second)
	defer ticker.Stop()
	for {
		threshold := GetEpochMillis() + int64(r.txnTimeout*1000)
		expiredTxns, err := r.master.QueryAll(context.Background(), "select * from `cluster_txn` where `timestamp`<?", NewSmartMapper(&ClusterTxn{}), threshold)
		if err != nil {
			zap.L().Error("failed to query expired transactions", zap.Error(err))
		} else {
			for _, txn := range expiredTxns {
				r.resolveTxn(txn.(*ClusterTxn).ID)
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
	xa.state = XAStateActive
	return err
}

func (xa *XATransaction) prepare() error {
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
	if xa.state != XAStatePrepared {
		return errors.New("commit is not allowed in this state " + strconv.Itoa(xa.state))
	}

	xa.execXACommand("XA COMMIT " + xa.id)
	xa.state = XAStateCommitted
	return nil
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

	xa.execXACommand("XA ROLLBACK " + xa.id)
	xa.state = XAStateRollback
	return nil
}

// Close release xa transaction
func (xa *XATransaction) Close() {
	if xa.state == XAStatePrepared {
		xa.resolver.resolve(xa.id)
	} else if xa.state == XAStateActive || xa.state == XAStateIdle {
		err := xa.rollback()
		zap.L().Error("failed to rollback faulted transaction", zap.Error(err), zap.String("txnID", xa.id))
	}

	xa.conn.Close()
}

// Insert insert entity to db
func (xa *XATransaction) Insert(entity interface{}) (sql.Result, error) {
	ty := reflect.TypeOf(entity)
	var r sql.Result
	var err error
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
	LogOperation(xa.ctx, "Update"+ty.Name(), func() error {
		if xa.state == XAStateUnknown {
			e := xa.Begin()
			if e != nil {
				return e
			}
		}

		ed := GetOrCreateEntityDescriptor(reflect.TypeOf(entity))
		args := ed.GetUpdateValues(entity)
		args = append(args, ed.GetKeyValue(entity))
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
	LogOperation(xa.ctx, "Delete"+ty.Name(), func() error {
		if xa.state == XAStateUnknown {
			e := xa.Begin()
			if e != nil {
				return e
			}
		}

		ed := GetOrCreateEntityDescriptor(reflect.TypeOf(entity))
		r, err = xa.conn.ExecContext(xa.ctx, ed.DeleteSQL, ed.GetKeyValue(entity))
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

// GetOne query one entity based on the prototype
func (xa *XATransaction) GetOne(proto interface{}, id interface{}) (interface{}, error) {
	ty := reflect.TypeOf(proto)
	mapper := NewSmartMapper(proto)
	var result interface{}
	var err error
	LogOperation(xa.ctx, "GetOne"+ty.Name(), func() error {
		if xa.state == XAStateUnknown {
			e := xa.Begin()
			if e != nil {
				return e
			}
		}

		ed := GetOrCreateEntityDescriptor(ty)
		result, err = QueryOne(xa.ctx, xa.conn, mapper, ed.GetOneSQL, id)
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
	id           string
	ctx          context.Context
	master       *DbAccessor
	partitions   []*DbAccessor
	participants map[int32]*XATransaction
	idGen        UniqueIDGenerator
	resolver     *unknownStateTxnResolver
}

func newMyClusterTxn(
	ctx context.Context,
	id string,
	master *DbAccessor,
	partitions []*DbAccessor,
	idGen UniqueIDGenerator,
	resolver *unknownStateTxnResolver) *MyClusterTxn {
	return &MyClusterTxn{
		id:           id,
		ctx:          ctx,
		master:       master,
		partitions:   partitions,
		participants: make(map[int32]*XATransaction),
		idGen:        idGen,
		resolver:     resolver,
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
	_, err := txn.master.Execute(txn.ctx, "update `cluster_txn` set `state`=? where `id`=?", ClusterTxnStateCommitted, txn.id)
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
	_, err = txn.master.Execute(txn.ctx, "delete from `cluster_txn` where `id`=?", txn.id)
	if err != nil {
		return err
	}

	_, err = txn.master.Execute(txn.ctx, "delete from `txn_participant` where `txn_id`=?", txn.id)
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
	_, err := txn.master.Execute(txn.ctx, "delete from `cluster_txn` where `id`=?", txn.id)
	if err != nil {
		return err
	}

	_, err = txn.master.Execute(txn.ctx, "delete from `txn_participant` where `txn_id`=?", txn.id)
	if err != nil {
		return err
	}

	return nil
}

// GetTxnByPartition get an XATransaction by partition number
func (txn *MyClusterTxn) GetTxnByPartition(partition int32) (*XATransaction, error) {
	t, ok := txn.participants[partition]
	if ok {
		return t, nil
	}

	_, err := txn.master.Execute(txn.ctx, "insert into `txn_participant`(`txn_id`, `partition`) values(?, ?)", txn.id, partition)
	if err != nil {
		return nil, err
	}

	conn, err := txn.partitions[int(partition)%len(txn.partitions)].Conn(txn.ctx)
	if err != nil {
		return nil, err
	}

	txn.participants[partition] = newXATransaction(txn.ctx, fmt.Sprintf("'%v','%v'", txn.id, partition), conn, txn.resolver)
	return txn.participants[partition], nil
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

// Update update entity to database
func (txn *MyClusterTxn) Update(entity interface{}) (sql.Result, error) {
	descriptor := GetOrCreateEntityDescriptor(reflect.TypeOf(entity))
	if descriptor.key != nil {
		v := reflect.ValueOf(entity)
		for v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		f := v.FieldByIndex(descriptor.key.field.field.Index)
		k, ok := f.Interface().(int64)
		if !ok {
			return nil, errors.New("Not able to update entity with non unique id key")
		}

		xa, err := txn.GetTxnByID(k)
		if err != nil {
			return nil, err
		}

		return xa.Update(entity)
	}

	return nil, errors.New("Not able to update entity that does not have a key")
}

// UpdateAt update entity at the specific partition
func (txn *MyClusterTxn) UpdateAt(partition int32, entity interface{}) (sql.Result, error) {
	xa, err := txn.GetTxnByPartition(partition)
	if err != nil {
		return nil, err
	}

	return xa.Update(entity)
}

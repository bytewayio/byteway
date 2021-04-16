package cypress

import (
	"context"
	"database/sql"
	"reflect"
	"time"

	"go.uber.org/zap"
)

const (
	txnStateUnknown = iota
	txnStateCommitted
	txnStateRollback
)

// Queryable a queryable object that could be a Connection, DB or Tx
type Queryable interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// DataRow data row, which can be used to scan values or get column information
type DataRow interface {
	ColumnTypes() ([]*sql.ColumnType, error)
	Columns() ([]string, error)
	Scan(dest ...interface{}) error
}

// RowMapper maps a row to an object
type RowMapper interface {
	Map(row DataRow) (interface{}, error)
}

// RowMapperFunc a function that implements RowMapper
type RowMapperFunc func(row DataRow) (interface{}, error)

// Map implements the RowMapper interface
func (mapper RowMapperFunc) Map(row DataRow) (interface{}, error) {
	return mapper(row)
}

// TableNameResolver resolves a struct name to table name
type TableNameResolver interface {
	Resolve(structName string) string
}

// TableNameResolverFunc a function that implements TableNameResolver
type TableNameResolverFunc func(structName string) string

// Resolve resolves struct name to table name
func (resolver TableNameResolverFunc) Resolve(name string) string {
	return resolver(name)
}

// LogExec log the sql Exec call result
func LogExec(activityID string, start time.Time, err error) {
	latency := time.Since(start)
	zap.L().Info("execSql", zap.Int("latency", int(latency.Seconds()*1000)), zap.Bool("success", err == nil), zap.String("activityId", activityID))
}

// QueryOne query one object
func QueryOne(ctx context.Context, queryable Queryable, mapper RowMapper, query string, args ...interface{}) (interface{}, error) {
	var err error
	start := time.Now()
	defer func(e error) {
		latency := time.Since(start)
		zap.L().Info("queryOne", zap.Int("latency", int(latency.Seconds()*1000)), zap.Bool("success", e == sql.ErrNoRows || e == nil), zap.String("activityId", GetTraceID(ctx)))
	}(err)
	rows, err := queryable.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	if !rows.Next() {
		return nil, nil
	}

	obj, err := mapper.Map(rows)
	if err != nil {
		return nil, err
	}

	return obj, nil
}

// QueryAll query all rows and map them to objects
func QueryAll(ctx context.Context, queryable Queryable, mapper RowMapper, query string, args ...interface{}) ([]interface{}, error) {
	var err error
	start := time.Now()
	defer func(e error) {
		latency := time.Since(start)
		zap.L().Info("queryAll", zap.Int("latency", int(latency.Seconds()*1000)), zap.Bool("success", e == sql.ErrNoRows || e == nil), zap.String("activityId", GetTraceID(ctx)))
	}(err)

	rows, err := queryable.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	results := make([]interface{}, 0, 10)
	for rows.Next() {
		obj, err := mapper.Map(rows)
		if err != nil {
			return nil, err
		}

		results = append(results, obj)
	}

	return results, nil
}

// DbTxn db transaction wrapper with context
type DbTxn struct {
	conn  *sql.Conn
	tx    *sql.Tx
	ctx   context.Context
	state int
}

// implement Txn for DbTxn

// Insert insert entity to db
func (txn *DbTxn) Insert(entity interface{}) (sql.Result, error) {
	ty := reflect.TypeOf(entity)
	var r sql.Result
	var err error
	for ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	LogOperation(txn.ctx, "Insert"+ty.Name(), func() error {
		ed := GetOrCreateEntityDescriptor(reflect.TypeOf(entity))
		r, err = txn.tx.ExecContext(txn.ctx, ed.InsertSQL, ed.GetInsertValues(entity)...)
		return err
	})

	return r, err
}

// Update update entity with db
func (txn *DbTxn) Update(entity interface{}) (sql.Result, error) {
	ty := reflect.TypeOf(entity)
	var r sql.Result
	var err error
	for ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	LogOperation(txn.ctx, "Update"+ty.Name(), func() error {
		ed := GetOrCreateEntityDescriptor(reflect.TypeOf(entity))
		args := ed.GetUpdateValues(entity)
		args = append(args, ed.GetKeyValues(entity)...)
		r, err = txn.tx.ExecContext(txn.ctx, ed.UpdateSQL, args...)
		return err
	})

	return r, err
}

// Delete delete entity from db
func (txn *DbTxn) Delete(entity interface{}) (sql.Result, error) {
	ty := reflect.TypeOf(entity)
	var r sql.Result
	var err error
	for ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	LogOperation(txn.ctx, "Delete"+ty.Name(), func() error {
		ed := GetOrCreateEntityDescriptor(reflect.TypeOf(entity))
		r, err = txn.tx.ExecContext(txn.ctx, ed.DeleteSQL, ed.GetKeyValues(entity)...)
		return err
	})

	return r, err
}

// Execute execute command towards db
func (txn *DbTxn) Execute(command string, args ...interface{}) (sql.Result, error) {
	var r sql.Result
	var err error
	LogOperation(txn.ctx, "ExecuteCommand", func() error {
		r, err = txn.tx.ExecContext(txn.ctx, command, args...)
		return err
	})

	return r, err
}

// GetOneByKey query one entity based on the key, the entity must have a sinle dimension key
func (txn *DbTxn) GetOneByKey(ty reflect.Type, key interface{}) (interface{}, error) {
	mapper := NewSmartMapper(ty)
	var result interface{}
	var err error
	for ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	LogOperation(txn.ctx, "GetOneByKey"+ty.Name(), func() error {
		ed := GetOrCreateEntityDescriptor(ty)
		result, err = QueryOne(txn.ctx, txn.tx, mapper, ed.GetOneSQL, key)
		return err
	})

	return result, err
}

// GetOne query one entity based on the key fields set in the prototype
func (txn *DbTxn) GetOne(proto interface{}) (interface{}, error) {
	ty := reflect.TypeOf(proto)
	mapper := NewSmartMapper(ty)
	var result interface{}
	var err error
	for ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	LogOperation(txn.ctx, "GetOne"+ty.Name(), func() error {
		ed := GetOrCreateEntityDescriptor(ty)
		result, err = QueryOne(txn.ctx, txn.tx, mapper, ed.GetOneSQL, ed.GetKeyValues(proto)...)
		return err
	})

	return result, err
}

// QueryOne execute sql query and return one entity based on the mapper
func (txn *DbTxn) QueryOne(query string, mapper RowMapper, args ...interface{}) (interface{}, error) {
	var result interface{}
	var err error
	LogOperation(txn.ctx, "QueryOne", func() error {
		result, err = QueryOne(txn.ctx, txn.tx, mapper, query, args...)
		return err
	})

	return result, err
}

// QueryAll execute sql query and return all entities based on the mapper
func (txn *DbTxn) QueryAll(query string, mapper RowMapper, args ...interface{}) ([]interface{}, error) {
	var result []interface{}
	var err error
	LogOperation(txn.ctx, "QueryAll", func() error {
		result, err = QueryAll(txn.ctx, txn.tx, mapper, query, args...)
		return err
	})

	return result, err
}

// Rollback rollback the transaction
func (txn *DbTxn) Rollback() error {
	err := txn.tx.Rollback()
	if err == nil {
		txn.state = txnStateRollback
	}

	return err
}

// Commit commit the transaction
func (txn *DbTxn) Commit() error {
	err := txn.tx.Commit()
	if err == nil {
		txn.state = txnStateCommitted
	}

	return err
}

// Close commit or rollback transaction and close conn
func (txn *DbTxn) Close() {
	if txn.state == txnStateUnknown {
		err := txn.tx.Rollback()
		if err != nil {
			zap.L().Error("failed to rollback transaction", zap.Error(err))
		}
	}

	err := txn.conn.Close()
	if err != nil {
		zap.L().Error("failed to close underlying conn", zap.Error(err))
	}
}

// DbAccessor database accessor
type DbAccessor struct {
	db *sql.DB
}

// NewDbAccessor create a new instance of data accessor for the given db
func NewDbAccessor(db *sql.DB) *DbAccessor {
	return &DbAccessor{db}
}

// Insert insert entity to db
func (accessor *DbAccessor) Insert(ctx context.Context, entity interface{}) (sql.Result, error) {
	ty := reflect.TypeOf(entity)
	var r sql.Result
	var err error
	for ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	LogOperation(ctx, "Insert"+ty.Name(), func() error {
		ed := GetOrCreateEntityDescriptor(reflect.TypeOf(entity))
		r, err = accessor.db.ExecContext(ctx, ed.InsertSQL, ed.GetInsertValues(entity)...)
		return err
	})

	return r, err
}

// Update update entity with db
func (accessor *DbAccessor) Update(ctx context.Context, entity interface{}) (sql.Result, error) {
	ty := reflect.TypeOf(entity)
	var r sql.Result
	var err error
	for ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	LogOperation(ctx, "Update"+ty.Name(), func() error {
		ed := GetOrCreateEntityDescriptor(reflect.TypeOf(entity))
		args := ed.GetUpdateValues(entity)
		args = append(args, ed.GetKeyValues(entity)...)
		r, err = accessor.db.ExecContext(ctx, ed.UpdateSQL, args...)
		return err
	})

	return r, err
}

// Delete delete entity from db
func (accessor *DbAccessor) Delete(ctx context.Context, entity interface{}) (sql.Result, error) {
	ty := reflect.TypeOf(entity)
	var r sql.Result
	var err error
	for ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	LogOperation(ctx, "Delete"+ty.Name(), func() error {
		ed := GetOrCreateEntityDescriptor(reflect.TypeOf(entity))
		r, err = accessor.db.ExecContext(ctx, ed.DeleteSQL, ed.GetKeyValues(entity)...)
		return err
	})

	return r, err
}

// Execute execute command towards db
func (accessor *DbAccessor) Execute(ctx context.Context, command string, args ...interface{}) (sql.Result, error) {
	var r sql.Result
	var err error
	LogOperation(ctx, "ExecuteCommand", func() error {
		r, err = accessor.db.ExecContext(ctx, command, args...)
		return err
	})

	return r, err
}

// GetOneByKey query one entity based on the given key, the entity must have a single dimension primary key
func (accessor *DbAccessor) GetOneByKey(ctx context.Context, ty reflect.Type, key interface{}) (interface{}, error) {
	mapper := NewSmartMapper(ty)
	var result interface{}
	var err error
	for ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	LogOperation(ctx, "GetOneByKey"+ty.Name(), func() error {
		ed := GetOrCreateEntityDescriptor(ty)
		result, err = QueryOne(ctx, accessor.db, mapper, ed.GetOneSQL, key)
		return err
	})

	return result, err
}

// GetOne query one entity based on the prototype
func (accessor *DbAccessor) GetOne(ctx context.Context, proto interface{}) (interface{}, error) {
	ty := reflect.TypeOf(proto)
	mapper := NewSmartMapper(ty)
	var result interface{}
	var err error
	for ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}

	LogOperation(ctx, "GetOne"+ty.Name(), func() error {
		ed := GetOrCreateEntityDescriptor(ty)
		result, err = QueryOne(ctx, accessor.db, mapper, ed.GetOneSQL, ed.GetKeyValues(proto)...)
		return err
	})

	return result, err
}

// QueryOne execute sql query and return one entity based on the mapper
func (accessor *DbAccessor) QueryOne(ctx context.Context, query string, mapper RowMapper, args ...interface{}) (interface{}, error) {
	var result interface{}
	var err error
	LogOperation(ctx, "QueryOne", func() error {
		result, err = QueryOne(ctx, accessor.db, mapper, query, args...)
		return err
	})

	return result, err
}

// QueryAll execute sql query and return all entities based on the mapper
func (accessor *DbAccessor) QueryAll(ctx context.Context, query string, mapper RowMapper, args ...interface{}) ([]interface{}, error) {
	var result []interface{}
	var err error
	LogOperation(ctx, "QueryAll", func() error {
		result, err = QueryAll(ctx, accessor.db, mapper, query, args...)
		return err
	})

	return result, err
}

// BeginTxnWithIsolation starts a new transaction
func (accessor *DbAccessor) BeginTxnWithIsolation(ctx context.Context, isolation sql.IsolationLevel) (*DbTxn, error) {
	conn, err := accessor.db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: isolation,
	})
	if err != nil {
		return nil, err
	}

	return &DbTxn{
		conn:  conn,
		tx:    tx,
		ctx:   ctx,
		state: txnStateUnknown,
	}, nil
}

// BeginTxn starts a new transaction with RepeatableRead isolation
func (accessor *DbAccessor) BeginTxn(ctx context.Context) (*DbTxn, error) {
	return accessor.BeginTxnWithIsolation(ctx, sql.LevelRepeatableRead)
}

// Conn gets a connection to the DB
func (accessor *DbAccessor) Conn(ctx context.Context) (*sql.Conn, error) {
	return accessor.db.Conn(ctx)
}

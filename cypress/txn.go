package cypress

import (
	"database/sql"
	"reflect"
)

// Txn an abstraction for db transaction as well as cluster based db transaction
type Txn interface {
	Insert(entity interface{}) (sql.Result, error)

	Update(entity interface{}) (sql.Result, error)

	Delete(entity interface{}) (sql.Result, error)

	Execute(sql string, args ...interface{}) (sql.Result, error)

	GetOneByKey(ty reflect.Type, key interface{}) (interface{}, error)

	GetOne(proto interface{}) (interface{}, error)

	QueryOne(sql string, mapper RowMapper, args ...interface{}) (interface{}, error)

	QueryAll(sql string, mapper RowMapper, args ...interface{}) ([]interface{}, error)
}

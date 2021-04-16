package cypress

import (
	"context"
	"database/sql"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

type testEntity struct {
	ID          int64  `col:"id" dtags:"key"`
	Name        string `col:"name" dtags:"partition,noupdate"`
	LastUpdated int64  `col:"last_update_ts"`
	Alias1      string `alias:"alias1"`
	Alias2      string `col:"alias2" dtags:"alias"`
}

type testEntity1 struct {
	ID          int64  `col:"id" dtags:"key"`
	Name        string `col:"name" dtags:"partition"`
	LastUpdated int64  `col:"last_update_ts"`
}

func TestEntityDescriptor(t *testing.T) {
	descriptor := GetOrCreateEntityDescriptor(reflect.TypeOf(&testEntity{}))
	t.Log(descriptor.InsertSQL)
	t.Log(descriptor.UpdateSQL)
	t.Log(descriptor.DeleteSQL)
	if descriptor.key == nil {
		t.Error("key execpted")
		return
	}

	if len(descriptor.fields) != 3 {
		t.Error("expected three fields")
		return
	}

	if len(descriptor.updateFields) != 1 {
		t.Error("expected only one update field")
		return
	}

	if len(descriptor.insertFields) != 3 {
		t.Error("expected three insert field")
		return
	}

	// Ensure partition key is not upatable
	descriptor = GetOrCreateEntityDescriptor(reflect.TypeOf(&testEntity1{}))
	if len(descriptor.updateFields) != 1 {
		t.Error("expected only one update field")
		return
	}
}

func TestEntityDescriptorWithQueries(t *testing.T) {
	testDbFile, err := ioutil.TempFile(os.TempDir(), "entitytst")
	if err != nil {
		t.Error("failed to create test db file", err)
		return
	}

	defer os.Remove(testDbFile.Name())

	db, err := sql.Open("sqlite3", testDbFile.Name())
	if err != nil {
		t.Error("failed to open the database file", err)
		return
	}

	defer db.Close()

	descriptor := GetOrCreateEntityDescriptor(reflect.TypeOf(&testEntity{}))
	t.Log(descriptor.InsertSQL)
	t.Log(descriptor.UpdateSQL)
	t.Log(descriptor.DeleteSQL)

	entity := &testEntity{
		ID:          100,
		Name:        "Tester",
		LastUpdated: 1990,
		Alias1:      "alias1",
		Alias2:      "alias2",
	}

	_, err = db.Exec("create table test_entity(id int PRIMARY KEY, name varchar(100), last_update_ts int)")
	if err != nil {
		t.Error("failed to create test table", err)
		return
	}

	_, err = db.Exec(descriptor.InsertSQL, descriptor.GetInsertValues(entity)...)
	if err != nil {
		t.Error("failed to insert object", err)
		return
	}

	e, err := QueryOne(context.Background(), db, NewSmartMapper(reflect.TypeOf((*testEntity)(nil))), descriptor.GetOneSQL, 100)
	if err != nil {
		t.Error("failed to get entity back", err)
		return
	}

	if e == nil {
		t.Error("entity not found")
		return
	}

	entity = e.(*testEntity)
	if entity.LastUpdated != 1990 {
		t.Error("unexpected value", entity.LastUpdated)
		return
	}

	if entity.Name != "Tester" {
		t.Error("unexpected value", entity.Name)
		return
	}

	entity.LastUpdated = 2000
	values := descriptor.GetUpdateValues(entity)
	values = append(values, entity.ID)
	_, err = db.Exec(descriptor.UpdateSQL, values...)
	if err != nil {
		t.Error("failed to execute update sql", err)
		return
	}

	e, err = QueryOne(context.Background(), db, NewSmartMapper(reflect.TypeOf((*testEntity)(nil))), descriptor.GetOneSQL, 100)
	if err != nil {
		t.Error("failed to get entity back", err)
		return
	}

	if e == nil {
		t.Error("entity not found")
		return
	}

	entity = e.(*testEntity)
	if entity.LastUpdated != 2000 {
		t.Error("unexpected value", entity.LastUpdated)
		return
	}

	_, err = db.Exec(descriptor.DeleteSQL, entity.ID)
	if err != nil {
		t.Error("failed to delete entry", err)
		return
	}

	e, err = QueryOne(context.Background(), db, NewSmartMapper(reflect.TypeOf((*testEntity)(nil))), descriptor.GetOneSQL, 100)
	if err != nil {
		t.Error("failed to get entity back", err)
		return
	}

	if e != nil {
		t.Error("entity must be deleted")
		return
	}
}

func TestDbAccessor(t *testing.T) {
	testDbFile, err := ioutil.TempFile(os.TempDir(), "dbaccessortst")
	if err != nil {
		t.Error("failed to create test db file", err)
		return
	}

	defer os.Remove(testDbFile.Name())

	db, err := sql.Open("sqlite3", testDbFile.Name())
	if err != nil {
		t.Error("failed to open the database file", err)
		return
	}

	defer db.Close()

	_, err = db.Exec("create table test_entity(id int PRIMARY KEY, name varchar(100), last_update_ts int)")
	if err != nil {
		t.Error("failed to create test table", err)
		return
	}

	entity := &testEntity{
		ID:          200,
		Name:        "DbAccessorTest",
		LastUpdated: 2020,
		Alias1:      "alias1",
		Alias2:      "alias2",
	}

	accessor := NewDbAccessor(db)
	_, err = accessor.Insert(context.Background(), entity)
	if err != nil {
		t.Error("failed to insert entity", err)
		return
	}

	one, err := accessor.GetOne(context.Background(), entity)
	if err != nil {
		t.Error("failed to get one from db", err)
		return
	}

	if one == nil {
		t.Error("unexpected nil returned")
		return
	}

	entity1 := one.(*testEntity)
	if entity1.LastUpdated != 2020 {
		t.Error("unexpected last update value", entity1.LastUpdated)
		return
	}

	entity.LastUpdated = 2022
	result, err := accessor.Update(context.Background(), entity)
	if err != nil {
		t.Error("failed to update entity", err)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected != 1 {
		t.Error("unexpected rows affected", rowsAffected)
		return
	}
}

func TestDbAccessorTxn(t *testing.T) {
	testDbFile, err := ioutil.TempFile(os.TempDir(), "dbtxntst")
	if err != nil {
		t.Error("failed to create test db file", err)
		return
	}

	defer os.Remove(testDbFile.Name())

	db, err := sql.Open("sqlite3", testDbFile.Name())
	if err != nil {
		t.Error("failed to open the database file", err)
		return
	}

	defer db.Close()

	_, err = db.Exec("create table test_entity(id int PRIMARY KEY, name varchar(100), last_update_ts int)")
	if err != nil {
		t.Error("failed to create test table", err)
		return
	}

	accessor := NewDbAccessor(db)
	failed := false
	func() {
		txn, err := accessor.BeginTxn(context.Background())
		if err != nil {
			t.Error("unexpected error for begin txn", err)
			return
		}

		defer txn.Close()
		entity := &testEntity{
			ID:          200,
			Name:        "DbAccessorTest",
			LastUpdated: 2020,
			Alias1:      "alias1",
			Alias2:      "alias2",
		}

		_, err = txn.Insert(entity)
		if err != nil {
			t.Error("failed to insert due to error", err)
			failed = true
			return
		}

		txn.Rollback()
	}()

	if failed {
		return
	}

	// last txn has rollbacked, so there is no entity with id 200
	one, err := accessor.GetOne(context.Background(), &testEntity{ID: 200})
	if err != nil {
		t.Error("failed to get one from db", err)
		return
	}

	if one != nil {
		t.Error("there should not be any instance with ID 200")
		return
	}

	func() {
		txn, err := accessor.BeginTxn(context.Background())
		if err != nil {
			t.Error("unexpected error for begin txn", err)
			return
		}

		defer txn.Close()
		entity := &testEntity{
			ID:          200,
			Name:        "DbAccessorTest",
			LastUpdated: 2020,
			Alias1:      "alias1",
			Alias2:      "alias2",
		}

		_, err = txn.Insert(entity)
		if err != nil {
			t.Error("failed to insert due to error", err)
			failed = true
			return
		}

		err = txn.Commit()
		if err != nil {
			t.Error("failed to commit transaction", err)
			failed = true
			return
		}
	}()

	if failed {
		return
	}

	// last txn has rollbacked, so there is no entity with id 200
	one, err = accessor.GetOne(context.Background(), &testEntity{ID: 200})
	if err != nil {
		t.Error("failed to get one from db", err)
		return
	}

	if one == nil {
		t.Error("transaction committed, there must be an instance with ID 200")
		return
	}
}

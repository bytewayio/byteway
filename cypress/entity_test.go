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

	e, err := QueryOne(context.Background(), db, NewSmartMapper(entity), descriptor.GetOneSQL, 100)
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

	e, err = QueryOne(context.Background(), db, NewSmartMapper(entity), descriptor.GetOneSQL, 100)
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
	e, err = QueryOne(context.Background(), db, NewSmartMapper(entity), descriptor.GetOneSQL, 100)
	if err != nil {
		t.Error("failed to get entity back", err)
		return
	}

	if e != nil {
		t.Error("entity must be deleted")
		return
	}
}

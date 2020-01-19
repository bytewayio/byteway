package cypress

import "testing"

import "reflect"

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

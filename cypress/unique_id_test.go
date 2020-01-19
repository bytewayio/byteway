package cypress

import "testing"

func TestUniqueID(t *testing.T) {
	id, err := NewUniqueID(1, 2, 3)
	if err != nil {
		t.Error("unexpected error", err)
		return
	}

	if id.Partition() != 2 {
		t.Error("unexpected partition", id.Partition())
		return
	}

	if int32(id.Value&SegmentedIDMask) != 3 {
		t.Error("unexpected segmented id", id.Value)
		return
	}

	if id.Value>>PooledIDBitWidth != 1 {
		t.Error("unexpected pooled id", id.Value)
		return
	}
}

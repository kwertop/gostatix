package filters

import (
	"reflect"
	"testing"
)

func TestCuckooFilterBasic(t *testing.T) {
	filter := NewCuckooFilterWithErrorRate(20, 4, 500, 0.01)
	filter.Insert([]byte("john"), false)
	filter.Insert([]byte("jane"), false)
	if filter.length != 2 {
		t.Errorf("filter length should be 2, instead found %v", filter.length)
	}
	bucketsLength := 0
	for b := range filter.buckets {
		bucketsLength += int(filter.buckets[b].Length())
	}
	if bucketsLength != 2 {
		t.Errorf("total elements insisde buckets should be 2, instead found %v", bucketsLength)
	}
}

func TestAddDifferentBuckets(t *testing.T) {
	filter := NewCuckooFilterWithErrorRate(20, 2, 500, 0.01)
	e := []byte("foo")
	filter.Insert(e, false)
	filter.Insert(e, false)
	filter.Insert(e, false)
	filter.Insert(e, false)
	_, fIndex, sIndex, _ := filter.getPositions(e)
	if filter.buckets[fIndex].IsFree() || filter.buckets[sIndex].IsFree() {
		t.Error("both buckets should be full")
	}
	if filter.length != 4 {
		t.Errorf("filter length should be 4, instead found %v", filter.length)
	}
	bucketsLength := 0
	for b := range filter.buckets {
		bucketsLength += int(filter.buckets[b].Length())
	}
	if bucketsLength != 4 {
		t.Errorf("total elements insisde buckets should be 4, instead found %v", bucketsLength)
	}
}

func TestRetries(t *testing.T) {
	filter := NewCuckooFilterWithRetries(10, 1, 3, 1)
	e := []byte("foo")
	fingerPrint, fIndex, sIndex, _ := filter.getPositions(e)
	filter.buckets[fIndex].Add("bar")
	filter.buckets[sIndex].Add("baz")
	filter.length += 2
	ok := filter.Insert(e, false)
	if !ok {
		t.Errorf("%v should get added in the filter", string(e))
	}
	bucketsLength := 0
	for b := range filter.buckets {
		bucket := filter.buckets[b]
		if bucket.Length() > 0 {
			elem := bucket.At(0)
			if elem != "bar" && elem != "baz" && elem != fingerPrint {
				t.Errorf("elem shuold be either \"bar\", \"baz\" or \"%s\", instead found %v", fingerPrint, elem)
			}
		}
		bucketsLength += int(bucket.Length())
	}
	if filter.length != 3 {
		t.Errorf("filter length should be 3, instead found %v", filter.length)
	}
	if bucketsLength != 3 {
		t.Errorf("total elements insisde buckets should be 3, instead found %v", bucketsLength)
	}
}

func TestFilterFull(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("filter should be full, and panic should occur")
		}
	}()

	filter := NewCuckooFilter(1, 1, 3)
	e := []byte("foo")
	filter.Insert(e, false)
	filter.Insert(e, false)
}

func TestInsertAndLookup(t *testing.T) {
	filter := NewCuckooFilterWithErrorRate(20, 4, 500, 0.01)
	filter.Insert([]byte("alice"), false)
	filter.Insert([]byte("andrew"), false)
	filter.Insert([]byte("bob"), false)
	filter.Insert([]byte("sam"), false)

	filter.Insert([]byte("alice"), false)
	filter.Insert([]byte("andrew"), false)
	filter.Insert([]byte("bob"), false)
	filter.Insert([]byte("sam"), false)

	ok1 := filter.Lookup([]byte("samx"))
	ok2 := filter.Lookup([]byte("samy"))
	ok3 := filter.Lookup([]byte("alice"))
	ok4 := filter.Lookup([]byte("joe"))

	if ok1 {
		t.Error("samx shouldn't be present in filter")
	}
	if ok2 {
		t.Error("samy shouldn't be present in filter")
	}
	if !ok3 {
		t.Error("alice should be present in filter")
	}
	if ok4 {
		t.Error("joe shouldn't be present in filter")
	}
}

func TestRemovePresent(t *testing.T) {
	filter := NewCuckooFilterWithErrorRate(20, 4, 500, 0.01)
	e1 := []byte("foo")
	e2 := []byte("bar")
	filter.Insert(e1, false)
	filter.Insert(e2, false)
	ok := filter.Remove([]byte("foo"))
	if !ok {
		t.Error("should be able to remove as e1 is in the filter")
	}
	ok = filter.Remove([]byte("foo"))
	if ok {
		t.Error("shouldn't be able to remove as e1 isn't in the filter")
	}
}

func TestRemoveNotPresent(t *testing.T) {
	filter := NewCuckooFilterWithErrorRate(20, 4, 500, 0.01)
	e1 := []byte("foo")
	filter.Insert(e1, false)
	ok := filter.Remove([]byte("bar"))
	if ok {
		t.Error("shouldn't be able to remove as \"bar\" isn't in the filter")
	}
}

func TestRollbackWhenFull(t *testing.T) {
	filter := NewCuckooFilter(5, 1, 3)
	ok := filter.Insert([]byte("one"), false)
	if !ok {
		t.Error("should insert one")
	}
	ok = filter.Insert([]byte("two"), false)
	if !ok {
		t.Error("should insert two")
	}
	ok = filter.Insert([]byte("three"), false)
	if !ok {
		t.Error("should insert three")
	}
	ok = filter.Insert([]byte("four"), false)
	if !ok {
		t.Error("should insert four")
	}
	ok = filter.Insert([]byte("five"), false)
	if !ok {
		t.Error("should insert five")
	}
	snapshot1, _ := filter.Export()

	defer func() {
		r := recover()
		if r == nil {
			t.Error("filter should be full, and panic should occur")
		}
		snapshot2, _ := filter.Export()
		if !reflect.DeepEqual(snapshot1, snapshot2) {
			t.Error("snapshot1 and snapshot2 should be equal")
		}
	}()

	ok = filter.Insert([]byte("six"), false)
	if !ok {
		t.Error("should insert six")
	}
}
func TestNoRollbackWhenFull(t *testing.T) {
	filter := NewCuckooFilter(5, 1, 3)
	ok := filter.Insert([]byte("one"), false)
	if !ok {
		t.Error("should insert one")
	}
	ok = filter.Insert([]byte("two"), false)
	if !ok {
		t.Error("should insert two")
	}
	ok = filter.Insert([]byte("three"), false)
	if !ok {
		t.Error("should insert three")
	}
	ok = filter.Insert([]byte("four"), false)
	if !ok {
		t.Error("should insert four")
	}
	ok = filter.Insert([]byte("five"), false)
	if !ok {
		t.Error("should insert five")
	}
	snapshot1, _ := filter.Export()

	defer func() {
		r := recover()
		if r == nil {
			t.Error("filter should be full, and panic should occur")
		}
		snapshot2, _ := filter.Export()
		if reflect.DeepEqual(snapshot1, snapshot2) {
			t.Error("snapshot1 and snapshot2 shouldn't be equal")
		}
	}()

	ok = filter.Insert([]byte("six"), true)
	if !ok {
		t.Error("should insert six")
	}
}

func TestCuckooImportInvalidJSON(t *testing.T) {
	data := []byte("{invalid}")

	var g CuckooFilter
	err := g.Import(data)
	if err == nil {
		t.Error("expected error while unmarshalling invalid data")
	}
}

func TestCuckooEquals(t *testing.T) {
	filter1 := NewCuckooFilter(5, 1, 3)
	filter1.Insert([]byte("one"), false)
	filter1.Insert([]byte("two"), false)
	filter1.Insert([]byte("three"), false)
	filter2 := NewCuckooFilter(5, 1, 3)
	filter2.Insert([]byte("one"), false)
	filter2.Insert([]byte("two"), false)
	filter2.Insert([]byte("three"), false)
	if !filter1.Equals(*filter2) {
		t.Error("filter1 and filter2 should be same")
	}
}

func TestCuckooMarshalUnmarshal(t *testing.T) {
	filter1 := NewCuckooFilter(5, 1, 3)
	filter1.Insert([]byte("one"), false)
	filter1.Insert([]byte("two"), false)
	filter1.Insert([]byte("three"), false)
	filter1.Insert([]byte("four"), false)
	snapshot1, _ := filter1.Export()
	filter2 := NewCuckooFilter(5, 1, 3)
	filter2.Insert([]byte("one"), false)
	filter2.Insert([]byte("two"), false)
	filter2.Insert([]byte("three"), false)
	filter2.Insert([]byte("four"), false)
	snapshot2, _ := filter2.Export()
	if !reflect.DeepEqual(snapshot1, snapshot2) {
		t.Error("snapshot1 and snapshot2 should be equal")
	}
	filter3 := NewCuckooFilter(0, 0, 0)
	filter3.Import(snapshot1)
	ok := filter3.Lookup([]byte("one"))
	if !ok {
		t.Error("\"one\" should be in filter3")
	}
	ok = filter3.Lookup([]byte("five"))
	if ok {
		t.Error("\"five\" should not be in filter3")
	}
	if !filter1.Equals(*filter3) || !filter2.Equals(*filter3) {
		t.Errorf("filter1, filter2 and filter3 are same")
	}
}

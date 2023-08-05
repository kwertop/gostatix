package filters

import (
	"testing"

	"github.com/gostatix/bitset"
)

func TestFilterSizeError(t *testing.T) {
	bitset := bitset.NewBitSetMem(1000)
	_, err := NewBloomFilterWithBitSet(100, 4, bitset)
	if err == nil {
		t.Error("should error out as size doesn't match")
	}
}

func TestFilterWithBitSetMem(t *testing.T) {
	bitset := bitset.NewBitSetMem(1000)
	filter, _ := NewBloomFilterWithBitSet(1000, 4, bitset)
	b1 := []byte("John")
	b2 := []byte("Jane")
	b3 := []byte("Alice")
	b4 := []byte("Bob")
	filter.Insert(b1)
	ok1 := filter.Lookup(b2)
	ok2 := filter.Lookup(b1)
	filter.Insert(b3)
	ok3 := filter.Lookup(b4)
	ok4 := filter.Lookup(b3)
	if ok1 {
		t.Errorf("%v should not be in filter", b2)
	}
	if !ok2 {
		t.Errorf("%v should be in filter", b1)
	}
	if ok3 {
		t.Errorf("%v should not be in filter", b4)
	}
	if !ok4 {
		t.Errorf("%v should be in filter", b3)
	}
}

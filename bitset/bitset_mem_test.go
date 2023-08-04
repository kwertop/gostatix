package bitset

import (
	"fmt"
	"testing"
)

func TestBitSetMemHas(t *testing.T) {
	bitset := NewBitSetMem(4)
	bitset.Insert(2)
	bitset.Insert(3)
	bitset.Insert(7)
	if ok, _ := bitset.Has(3); !ok {
		t.Fatalf("should be true at index 3, got %v", ok)
	}
	if ok, _ := bitset.Has(4); ok {
		t.Fatalf("should be false at index 4, got %v", ok)
	}
}

func TestBitSetMemFromData(t *testing.T) {
	bitset := FromDataMem([]uint64{3, 10})
	if ok, _ := bitset.Has(0); !ok {
		t.Fatalf("should be true at index 0, got %v", ok)
	}
	if ok, _ := bitset.Has(1); !ok {
		t.Fatalf("should be true at index 1, got %v", ok)
	}
	if ok, _ := bitset.Has(2); ok {
		t.Fatalf("should be false at index 2, got %v", ok)
	}
	if ok, _ := bitset.Has(63); ok {
		t.Fatalf("should be false at index 63, got %v", ok)
	}
	if ok, _ := bitset.Has(64); ok {
		t.Fatalf("should be false at index 64, got %v", ok)
	}
	if ok, _ := bitset.Has(65); !ok {
		t.Fatalf("should be false at index 65, got %v", ok)
	}
	if ok, _ := bitset.Has(66); ok {
		t.Fatalf("should be false at index 66, got %v", ok)
	}
}

func TestBitSetMemSetBits(t *testing.T) {
	bitset := FromDataMem([]uint64{3, 10})
	setBits, _ := bitset.BitCount()
	if setBits != 4 {
		t.Fatalf("count of set bits should be 4, got %v", setBits)
	}
}

func TestBitSetMemExport(t *testing.T) {
	bitset := NewBitSetMem(6)
	bitset.Insert(1)
	bitset.Insert(5)
	bitset.Insert(8)
	size, data, _ := bitset.Export()
	str := "\"AAAAAAAAAAYAAAAAAAABIg==\""
	if size != 6 {
		t.Fatalf("size of bitset should be 6, got %v", size)
	}
	if string(data) != str {
		t.Fatalf("exported string don't match %v, %v", string(data), str)
	}
}

func TestBitSetMemImport(t *testing.T) {
	bitset := NewBitSetMem(6)
	str := "\"AAAAAAAAAAYAAAAAAAABIg==\""
	bitset.Import(6, []byte(str))
	if ok, _ := bitset.Has(0); ok {
		t.Fatalf("should be false at index 0, got %v", ok)
	}
	if ok, _ := bitset.Has(1); !ok {
		t.Fatalf("should be true at index 1, got %v", ok)
	}
	if ok, _ := bitset.Has(5); !ok {
		t.Fatalf("should be true at index 5, got %v", ok)
	}
	if ok, _ := bitset.Has(7); ok {
		t.Fatalf("should be false at index 1, got %v", ok)
	}
}

func TestBitSetMemNotEqual(t *testing.T) {
	aBitset := NewBitSetMem(0)
	bBitset := NewBitSetRedis(0, "k")
	if ok, _ := aBitset.Equals(bBitset); ok {
		t.Fatal("aBitset and bBitset shouldn't be equal")
	}
}

func TestBitSetMemEqual(t *testing.T) {
	aBitset := NewBitSetMem(3)
	aBitset.Insert(0)
	aBitset.Insert(1)
	bBitset := NewBitSetMem(3)
	bBitset.Insert(0)
	bBitset.Insert(1)
	ok, err := aBitset.Equals(bBitset)
	if err != nil {
		fmt.Printf("error: %v", err)
	}
	if !ok {
		t.Fatal("aBitset and bBitset should be equal")
	}
}

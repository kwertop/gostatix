package gostatix

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func TestBitSetMemHas(t *testing.T) {
	bitset := newBitSetMem(4)
	bitset.insert(2)
	bitset.insert(3)
	bitset.insert(7)
	if ok, _ := bitset.has(3); !ok {
		t.Fatalf("should be true at index 3, got %v", ok)
	}
	if ok, _ := bitset.has(4); ok {
		t.Fatalf("should be false at index 4, got %v", ok)
	}
}

func TestBitSetMemFromData(t *testing.T) {
	bitset := fromDataMem([]uint64{3, 10})
	if ok, _ := bitset.has(0); !ok {
		t.Fatalf("should be true at index 0, got %v", ok)
	}
	if ok, _ := bitset.has(1); !ok {
		t.Fatalf("should be true at index 1, got %v", ok)
	}
	if ok, _ := bitset.has(2); ok {
		t.Fatalf("should be false at index 2, got %v", ok)
	}
	if ok, _ := bitset.has(63); ok {
		t.Fatalf("should be false at index 63, got %v", ok)
	}
	if ok, _ := bitset.has(64); ok {
		t.Fatalf("should be false at index 64, got %v", ok)
	}
	if ok, _ := bitset.has(65); !ok {
		t.Fatalf("should be false at index 65, got %v", ok)
	}
	if ok, _ := bitset.has(66); ok {
		t.Fatalf("should be false at index 66, got %v", ok)
	}
}

func TestBitSetMemSetBits(t *testing.T) {
	bitset := fromDataMem([]uint64{3, 10})
	setBits, _ := bitset.bitCount()
	if setBits != 4 {
		t.Fatalf("count of set bits should be 4, got %v", setBits)
	}
}

func TestBitSetMemExport(t *testing.T) {
	bitset := newBitSetMem(6)
	bitset.insert(1)
	bitset.insert(5)
	bitset.insert(8)
	size, data, _ := bitset.marshal()
	str := "\"AAAAAAAAAAkAAAAAAAABIg==\""
	if size != 6 {
		t.Fatalf("size of bitset should be 6, got %v", size)
	}
	if string(data) != str {
		t.Fatalf("exported string don't match %v, %v", string(data), str)
	}
}

func TestBitSetMemImport(t *testing.T) {
	bitset := newBitSetMem(6)
	str := "\"AAAAAAAAAAYAAAAAAAABIg==\""
	bitset.unmarshal([]byte(str))
	if ok, _ := bitset.has(0); ok {
		t.Fatalf("should be false at index 0, got %v", ok)
	}
	if ok, _ := bitset.has(1); !ok {
		t.Fatalf("should be true at index 1, got %v", ok)
	}
	if ok, _ := bitset.has(5); !ok {
		t.Fatalf("should be true at index 5, got %v", ok)
	}
	if ok, _ := bitset.has(7); ok {
		t.Fatalf("should be false at index 1, got %v", ok)
	}
}

func TestBitSetMemNotEqual(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := ParseRedisURI(redisUri)
	MakeRedisClient(*connOptions)
	aBitset := newBitSetMem(0)
	bBitset := newBitSetRedis(0)
	if ok, _ := aBitset.equals(bBitset); ok {
		t.Fatal("aBitset and bBitset shouldn't be equal")
	}
}

func TestBitSetMemEqual(t *testing.T) {
	aBitset := newBitSetMem(3)
	aBitset.insert(0)
	aBitset.insert(1)
	bBitset := newBitSetMem(3)
	bBitset.insert(0)
	bBitset.insert(1)
	ok, err := aBitset.equals(bBitset)
	if err != nil {
		fmt.Printf("error: %v", err)
	}
	if !ok {
		t.Fatal("aBitset and bBitset should be equal")
	}
}

func TestBitSetMemBinaryReadWrite(t *testing.T) {
	aBitset := newBitSetMem(6)
	aBitset.insert(1)
	aBitset.insert(5)
	aBitset.insert(8)

	var buff bytes.Buffer
	_, err := aBitset.writeTo(&buff)
	if err != nil {
		t.Error("error should be nil during binary write")
	}

	bBitSet := &BitSetMem{}
	_, err = bBitSet.readFrom(&buff)
	if err != nil {
		t.Error("error should be nil during binary read")
	}

	if ok, _ := aBitset.equals(bBitSet); !ok {
		t.Error("aBitset and bBitset should be equal")
	}

	if ok, _ := bBitSet.has(0); ok {
		t.Fatalf("should be false at index 0, got %v", ok)
	}
	if ok, _ := bBitSet.has(1); !ok {
		t.Fatalf("should be true at index 1, got %v", ok)
	}
	if ok, _ := bBitSet.has(5); !ok {
		t.Fatalf("should be true at index 5, got %v", ok)
	}
}

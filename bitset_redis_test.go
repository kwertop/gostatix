package gostatix

import (
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func TestBitSetRedisHas(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := ParseRedisURI(redisUri)
	MakeRedisClient(*connOptions)
	bitset := newBitSetRedis(4)
	bitset.insert(1)
	bitset.insert(3)
	bitset.insert(7)
	if ok, _ := bitset.has(1); !ok {
		t.Fatalf("should be true at index 1, got %v", ok)
	}
	if ok, _ := bitset.has(4); ok {
		t.Fatalf("should be false at index 4, got %v", ok)
	}
}

func TestBitSetRedisInsertMulti(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := ParseRedisURI(redisUri)
	MakeRedisClient(*connOptions)
	bitset := newBitSetRedis(4)
	indexes := []uint{1, 3, 7, 9}
	bitset.insertMulti(indexes)
	if ok, _ := bitset.has(1); !ok {
		t.Fatalf("should be true at index 1, got %v", ok)
	}
	if ok, _ := bitset.has(4); ok {
		t.Fatalf("should be false at index 4, got %v", ok)
	}
}

func TestBitSetRedisHasMulti(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := ParseRedisURI(redisUri)
	MakeRedisClient(*connOptions)
	bitset := newBitSetRedis(4)
	bitset.insert(1)
	bitset.insert(3)
	bitset.insert(7)
	bitset.insert(9)
	has, _ := bitset.hasMulti([]uint{1, 3, 7, 9})
	if !has[1] {
		t.Fatalf("should be true at index 3, got %v", has[1])
	}
	if !has[3] {
		t.Fatalf("should be false at index 9, got %v", has[4])
	}
}

func TestBitSetRedisFromData(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := ParseRedisURI(redisUri)
	MakeRedisClient(*connOptions)
	bitset, _ := fromDataRedis([]uint64{3, 10})
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

func TestBitSetRedisSetBits(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := ParseRedisURI(redisUri)
	MakeRedisClient(*connOptions)
	bitset, _ := fromDataRedis([]uint64{3, 10})
	setBits, _ := bitset.bitCount()
	if setBits != 4 {
		t.Fatalf("count of set bits should be 4, got %v", setBits)
	}
}

func TestBitSetRedisExport(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := ParseRedisURI(redisUri)
	MakeRedisClient(*connOptions)
	bitset := newBitSetRedis(8)
	bitset.insert(1)
	bitset.insert(5)
	bitset.insert(8)
	size, data, _ := bitset.marshal()
	str := "\"AAAAAAAAAAgAAAAAAAABIg==\""
	if size != 8 {
		t.Fatalf("size of bitset should be 1, got %v", size)
	}
	if string(data) != str {
		t.Fatalf("exported string don't match found %v, should be %v", string(data), str)
	}
}

func TestBitSetRedisImport(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := ParseRedisURI(redisUri)
	MakeRedisClient(*connOptions)
	str := "\"AAAAAAAAAAEAAAAAAAABIg==\""
	bitset := newBitSetRedis(1)
	ok, _ := bitset.unmarshal([]byte(str))
	if !ok {
		t.Fatalf("import failed for %v", str)
	}
	if ok, _ := bitset.has(0); ok {
		t.Fatalf("should be false at index 0, got %v", ok)
	}
	if ok, _ := bitset.has(1); !ok {
		t.Fatalf("should be true at index 1, got %v", ok)
	}
	if ok, _ := bitset.has(5); !ok {
		t.Fatalf("should be true at index 5, got %v", ok)
	}
	if ok, _ := bitset.has(8); !ok {
		t.Fatalf("should be true at index 8, got %v", ok)
	}
	if ok, _ := bitset.has(10); ok {
		t.Fatalf("should be false at index 10, got %v", ok)
	}
}

func TestBitSetRedisNotEqual(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := ParseRedisURI(redisUri)
	MakeRedisClient(*connOptions)
	aBitset := newBitSetRedis(1)
	bBitset := newBitSetMem(1)
	if ok, _ := aBitset.equals(bBitset); ok {
		t.Fatal("aBitset and bBitset shouldn't be equal")
	}
}

func TestBitSetRedisEqual(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := ParseRedisURI(redisUri)
	MakeRedisClient(*connOptions)
	aBitset := newBitSetRedis(3)
	aBitset.insert(0)
	aBitset.insert(1)
	bBitset := newBitSetRedis(3)
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

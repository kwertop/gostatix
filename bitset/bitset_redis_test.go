package bitset

import (
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/kwertop/gostatix"
)

func TestBitSetRedisHas(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := gostatix.ParseRedisURI(redisUri)
	gostatix.MakeRedisClient(*connOptions)
	bitset := NewBitSetRedis(4)
	bitset.Insert(1)
	bitset.Insert(3)
	bitset.Insert(7)
	if ok, _ := bitset.Has(1); !ok {
		t.Fatalf("should be true at index 1, got %v", ok)
	}
	if ok, _ := bitset.Has(4); ok {
		t.Fatalf("should be false at index 4, got %v", ok)
	}
}

func TestBitSetRedisFromData(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := gostatix.ParseRedisURI(redisUri)
	gostatix.MakeRedisClient(*connOptions)
	bitset, _ := FromDataRedis([]uint64{3, 10})
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

func TestBitSetRedisSetBits(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := gostatix.ParseRedisURI(redisUri)
	gostatix.MakeRedisClient(*connOptions)
	bitset, _ := FromDataRedis([]uint64{3, 10})
	setBits, _ := bitset.BitCount()
	if setBits != 4 {
		t.Fatalf("count of set bits should be 4, got %v", setBits)
	}
}

func TestBitSetRedisExport(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := gostatix.ParseRedisURI(redisUri)
	gostatix.MakeRedisClient(*connOptions)
	bitset := NewBitSetRedis(1)
	bitset.Insert(1)
	bitset.Insert(5)
	bitset.Insert(8)
	size, data, _ := bitset.Export()
	str := "\"AAAAAAAAAAEAAAAAAAABIg==\""
	if size != 1 {
		t.Fatalf("size of bitset should be 1, got %v", size)
	}
	if string(data) != str {
		t.Fatalf("exported string don't match %v, %v", string(data), str)
	}
}

func TestBitSetRedisImport(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := gostatix.ParseRedisURI(redisUri)
	gostatix.MakeRedisClient(*connOptions)
	str := "\"AAAAAAAAAAEAAAAAAAABIg==\""
	bitset := NewBitSetRedis(1)
	ok, _ := bitset.Import([]byte(str))
	if !ok {
		t.Fatalf("import failed for %v", str)
	}
	if ok, _ := bitset.Has(0); ok {
		t.Fatalf("should be false at index 0, got %v", ok)
	}
	if ok, _ := bitset.Has(1); !ok {
		t.Fatalf("should be true at index 1, got %v", ok)
	}
	if ok, _ := bitset.Has(5); !ok {
		t.Fatalf("should be true at index 5, got %v", ok)
	}
	if ok, _ := bitset.Has(8); !ok {
		t.Fatalf("should be true at index 8, got %v", ok)
	}
	if ok, _ := bitset.Has(10); ok {
		t.Fatalf("should be false at index 10, got %v", ok)
	}
}

func TestBitSetRedisNotEqual(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := gostatix.ParseRedisURI(redisUri)
	gostatix.MakeRedisClient(*connOptions)
	aBitset := NewBitSetRedis(1)
	bBitset := NewBitSetMem(1)
	if ok, _ := aBitset.Equals(bBitset); ok {
		t.Fatal("aBitset and bBitset shouldn't be equal")
	}
}

func TestBitSetRedisEqual(t *testing.T) {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := gostatix.ParseRedisURI(redisUri)
	gostatix.MakeRedisClient(*connOptions)
	aBitset := NewBitSetRedis(3)
	aBitset.Insert(0)
	aBitset.Insert(1)
	bBitset := NewBitSetRedis(3)
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

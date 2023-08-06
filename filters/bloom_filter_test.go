package filters

import (
	"encoding/binary"
	"testing"

	"github.com/kwertop/gostatix/bitset"
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

func TestFilterZeroSizes(t *testing.T) {
	bitset := bitset.NewBitSetMem(0)
	filter, _ := NewBloomFilterWithBitSet(0, 0, bitset)
	if filter.GetCap() != 1 {
		t.Errorf("size: %v should be 1", filter.GetCap())
	}
	if filter.GetNumHashes() != 1 {
		t.Errorf("numHash: %v should be 1", filter.GetNumHashes())
	}
}

func TestInt32(t *testing.T) {
	bitset := bitset.NewBitSetMem(1000)
	filter, _ := NewBloomFilterWithBitSet(1000, 4, bitset)
	e1 := make([]byte, 4)
	e2 := make([]byte, 4)
	e3 := make([]byte, 4)
	binary.BigEndian.PutUint32(e1, 100)
	binary.BigEndian.PutUint32(e2, 101)
	binary.BigEndian.PutUint32(e3, 102)
	filter.Insert(e1)
	ok1 := filter.Lookup(e1)
	ok2 := filter.Lookup(e2)
	filter.Insert(e3)
	ok3 := filter.Lookup(e3)
	if !ok1 {
		t.Errorf("%v should be in filter", e1)
	}
	if ok2 {
		t.Errorf("%v should not be in filter", e2)
	}
	if !ok3 {
		t.Errorf("%v should be in filter", e3)
	}
}

func TestStringInFilter(t *testing.T) {
	bitset := bitset.NewBitSetMem(1000)
	filter, _ := NewBloomFilterWithBitSet(1000, 4, bitset)
	e1 := "This"
	e2 := "is"
	e3 := "present"
	e4 := "in"
	e5 := "bloom"
	filter.InsertString(e1)
	ok1 := filter.LookupString(e1)
	ok2 := filter.LookupString(e2)
	filter.InsertString(e3)
	ok3 := filter.LookupString(e3)
	ok4 := filter.LookupString(e4)
	filter.InsertString(e5)
	if !ok1 {
		t.Errorf("%v should be in filter", e1)
	}
	if ok2 {
		t.Errorf("%v should not be in filter", e2)
	}
	if !ok3 {
		t.Errorf("%v should be in filter", e3)
	}
	if ok4 {
		t.Errorf("%v should not be in filter", e4)
	}
}

func testPositiveRate(nItems uint, errorRate float64, t *testing.T) {
	filter, _ := NewMemBloomFilterWithParameters(nItems, errorRate)
	e := make([]byte, 4)
	for i := uint32(0); i < uint32(nItems); i++ {
		binary.BigEndian.PutUint32(e, i)
		filter.Insert(e)
	}
	estimatedErrorRate := filter.BloomPositiveRate()
	if estimatedErrorRate > 1.1*errorRate {
		t.Errorf("estimated error rate %v too high for nItems %v and expected error rate %v", estimatedErrorRate, nItems, errorRate)
	}
}

func TestPositiveRate1000_0001(t *testing.T) {
	testPositiveRate(1000, 0.001, t)
}

func TestPositiveRate10000_0001(t *testing.T) {
	testPositiveRate(10000, 0.001, t)
}

func TestPositiveRate100000_0001(t *testing.T) {
	testPositiveRate(100000, 0.001, t)
}

func TestPositiveRate1000_001(t *testing.T) {
	testPositiveRate(1000, 0.01, t)
}

func TestPositiveRate10000_001(t *testing.T) {
	testPositiveRate(10000, 0.01, t)
}

func TestPositiveRate100000_001(t *testing.T) {
	testPositiveRate(100000, 0.01, t)
}

func TestPositiveRate1000_01(t *testing.T) {
	testPositiveRate(1000, 0.1, t)
}

func TestPositiveRate10000_01(t *testing.T) {
	testPositiveRate(10000, 0.1, t)
}

func TestPositiveRate100000_01(t *testing.T) {
	testPositiveRate(100000, 0.1, t)
}

func TestGetSize(t *testing.T) {
	bitset := bitset.NewBitSetMem(1000)
	filter, _ := NewBloomFilterWithBitSet(1000, 4, bitset)
	if filter.GetCap() != filter.size {
		t.Errorf("getcap method return value %v doesn't match with filter size %v", filter.GetCap(), filter.size)
	}
}

func TestGetNumHashes(t *testing.T) {
	bitset := bitset.NewBitSetMem(1000)
	filter, _ := NewBloomFilterWithBitSet(1000, 4, bitset)
	if filter.GetNumHashes() != filter.numHashes {
		t.Errorf("getnumhashes method return value %v doesn't match with filter numHashes %v", filter.GetNumHashes(), filter.numHashes)
	}
}

func TestNotEqualsSize(t *testing.T) {
	aBitset := bitset.NewBitSetMem(1000)
	aFilter, _ := NewBloomFilterWithBitSet(1000, 4, aBitset)
	bBitset := bitset.NewBitSetMem(100)
	bFilter, _ := NewBloomFilterWithBitSet(100, 4, bBitset)
	if ok, _ := aFilter.Equals(*bFilter); ok {
		t.Errorf("aFilter and bFilter shouldn't be equal")
	}
}

func TestNotEqualsNumHashes(t *testing.T) {
	aBitset := bitset.NewBitSetMem(1000)
	aFilter, _ := NewBloomFilterWithBitSet(1000, 4, aBitset)
	bBitset := bitset.NewBitSetMem(100)
	bFilter, _ := NewBloomFilterWithBitSet(100, 6, bBitset)
	if ok, _ := aFilter.Equals(*bFilter); ok {
		t.Errorf("aFilter and bFilter shouldn't be equal")
	}
}

func TestEquals(t *testing.T) {
	size, numHashes := 1000, 4
	aBitset := bitset.NewBitSetMem(uint(size))
	aFilter, _ := NewBloomFilterWithBitSet(uint(size), uint(numHashes), aBitset)
	e := make([]byte, 4)
	for i := uint32(0); i < uint32(size); i++ {
		binary.BigEndian.PutUint32(e, i)
		aFilter.Insert(e)
	}
	bBitset := bitset.NewBitSetMem(uint(size))
	bFilter, _ := NewBloomFilterWithBitSet(uint(size), uint(numHashes), bBitset)
	for i := uint32(0); i < uint32(size); i++ {
		binary.BigEndian.PutUint32(e, i)
		bFilter.Insert(e)
	}
	if ok, _ := aFilter.Equals(*bFilter); !ok {
		t.Errorf("aFilter and bFilter should be equal")
	}
}

func TestExportImport(t *testing.T) {
	aBitset := bitset.NewBitSetMem(1000)
	afilter, _ := NewBloomFilterWithBitSet(1000, 4, aBitset)
	e1 := "This"
	e2 := "is"
	e3 := "present"
	e4 := "in"
	e5 := "bloom"
	afilter.InsertString(e1)
	afilter.InsertString(e2)
	afilter.InsertString(e4)
	afilter.InsertString(e5)
	exportedFilter, _ := afilter.Export()
	bBitset := bitset.NewBitSetMem(1000)
	bFilter, _ := NewBloomFilterWithBitSet(1000, 4, bBitset)
	bFilter.Import(exportedFilter)
	ok1 := bFilter.LookupString(e1)
	ok2 := bFilter.LookupString(e2)
	ok3 := bFilter.LookupString(e3)
	ok4 := bFilter.LookupString("blooms")
	if !ok1 {
		t.Errorf("%v should be in the filter.", e1)
	}
	if !ok2 {
		t.Errorf("%v should be in the filter.", e2)
	}
	if ok3 {
		t.Errorf("%v should not be in the filter.", e3)
	}
	if ok4 {
		t.Errorf("%v should not be in the filter.", "blooms")
	}
}

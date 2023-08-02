package filters

import (
	"math"
	"math/rand"

	"github.com/gostatix"
	"github.com/gostatix/buckets"
)

type CuckooFilter struct {
	filter []buckets.BucketMem
	*AbstractCuckooFilter
}

type Entry struct {
	fingerPrint string
	firstIndex  uint64
	secondIndex uint64
}

func NewCuckooFilter(size, bucketSize, fingerPrintLength uint64) *CuckooFilter {
	filter := make([]buckets.BucketMem, size)
	for i := range filter {
		filter[i] = *buckets.NewBucketMem(bucketSize)
	}
	baseFilter := MakeAbstractCuckooFilter(size, bucketSize, fingerPrintLength, 0, 500)
	return &CuckooFilter{filter, baseFilter}
}

func NewCuckooFilterWithRetries(size, bucketSize, fingerPrintLength, retries uint64) *CuckooFilter {
	filter := make([]buckets.BucketMem, size)
	for i := range filter {
		filter[i] = *buckets.NewBucketMem(bucketSize)
	}
	baseFilter := MakeAbstractCuckooFilter(size, bucketSize, fingerPrintLength, 0, retries)
	return &CuckooFilter{filter, baseFilter}
}

func NewCuckooFilterWithErrorRate(size, bucketSize, retries uint64, errorRate float64) *CuckooFilter {
	fingerPrintLength := gostatix.CalculateFingerPrintLength(size, errorRate)
	capacity := uint64(math.Ceil(float64(size) * 0.955 / float64(bucketSize)))
	return NewCuckooFilterWithRetries(capacity, bucketSize, fingerPrintLength, retries)
}

func (cuckooFilter *CuckooFilter) Insert(data []byte, destructive bool) bool {
	fingerPrint, fIndex, sIndex, _ := cuckooFilter.getPositions(data)
	if cuckooFilter.filter[fIndex].IsFree() {
		cuckooFilter.filter[fIndex].Add(fingerPrint)
	} else if cuckooFilter.filter[sIndex].IsFree() {
		cuckooFilter.filter[sIndex].Add(fingerPrint)
	} else {
		var index uint64
		if rand.Float32() < 0.5 {
			index = fIndex
		} else {
			index = sIndex
		}
		currFingerPrint := fingerPrint
		var items []Entry
		for i := uint64(0); i < cuckooFilter.retries; i++ {
			randIndex := uint64(math.Ceil(rand.Float64() * float64(cuckooFilter.filter[index].Length()-1)))
			prevFingerPrint := cuckooFilter.filter[index].At(index)
			items = append(items, Entry{prevFingerPrint, index, randIndex})
			cuckooFilter.filter[index].Set(randIndex, currFingerPrint)
			hash := getHash([]byte(prevFingerPrint))
			newIndex := (index ^ hash) % uint64(len(cuckooFilter.filter))
			if cuckooFilter.filter[newIndex].IsFree() {
				cuckooFilter.filter[newIndex].Add(prevFingerPrint)
				cuckooFilter.length++
				return true
			}
		}
		if !destructive {
			for i := len(items); i >= 0; i-- {
				item := items[i]
				cuckooFilter.filter[item.firstIndex].Set(item.secondIndex, item.fingerPrint)
			}
		}
		panic("cannot insert element, cuckoofilter is full")
	}
	cuckooFilter.length++
	return true
}

func (cuckooFilter *CuckooFilter) Lookup(data []byte) bool {
	fingerPrint, fIndex, sIndex, _ := cuckooFilter.getPositions(data)
	return cuckooFilter.filter[fIndex].Lookup(fingerPrint) ||
		cuckooFilter.filter[sIndex].Lookup(fingerPrint)
}

func (cuckooFilter *CuckooFilter) Remove(data []byte) bool {
	fingerPrint, fIndex, sIndex, _ := cuckooFilter.getPositions(data)
	if cuckooFilter.filter[fIndex].Lookup(fingerPrint) {
		cuckooFilter.filter[fIndex].Remove(fingerPrint)
		cuckooFilter.length--
		return true
	} else if cuckooFilter.filter[sIndex].Lookup(fingerPrint) {
		cuckooFilter.filter[sIndex].Remove(fingerPrint)
		cuckooFilter.length--
		return true
	} else {
		return false
	}
}

func (aFilter CuckooFilter) Equals(bFilter CuckooFilter) bool {
	count := 0
	result := true
	for result && count < len(aFilter.filter) {
		bucket := aFilter.filter[count]
		if !bFilter.filter[count].Equals(bucket) {
			return false
		}
	}
	return true
}

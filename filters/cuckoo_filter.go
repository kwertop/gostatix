package filters

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"

	"github.com/gostatix"
	"github.com/gostatix/buckets"
	"github.com/gostatix/hash"
)

type CuckooFilter struct {
	filter            []buckets.BucketMem
	size              uint64
	bucketSize        uint64
	fingerPrintLength uint64
	length            uint64
	retries           uint64
}

type Entry struct {
	fingerPrint string
	firstIndex  uint64
	secondIndex uint64
}

func NewCuckooFilter(size, bucketSize, fingerPrintLength uint64) *CuckooFilter {
	filter := make([]buckets.BucketMem, bucketSize)
	return &CuckooFilter{filter, size, bucketSize, fingerPrintLength, 0, 500}
}

func NewCuckooFilterWithMaxKicks(size, bucketSize, fingerPrintLength, maxKicks uint64) *CuckooFilter {
	filter := make([]buckets.BucketMem, bucketSize)
	return &CuckooFilter{filter, size, bucketSize, fingerPrintLength, 0, maxKicks}
}

func NewCuckooFilterWithErrorRate(size, bucketSize, maxKicks uint64, errorRate float64) *CuckooFilter {
	fingerPrintLength := gostatix.CalculateFingerPrintLength(size, errorRate)
	capacity := uint64(math.Ceil(float64(size) * 0.955 / float64(bucketSize)))
	return NewCuckooFilterWithMaxKicks(capacity, bucketSize, fingerPrintLength, maxKicks)
}

func (cuckooFilter CuckooFilter) Size() uint64 {
	return cuckooFilter.size
}

func (cuckooFilter CuckooFilter) Length() uint64 {
	return cuckooFilter.length
}

func (cuckooFilter CuckooFilter) BucketSize() uint64 {
	return cuckooFilter.bucketSize
}

func (cuckooFilter CuckooFilter) FingerPrintLength() uint64 {
	return cuckooFilter.fingerPrintLength
}

func (cuckooFilter CuckooFilter) CellSize() uint64 {
	return cuckooFilter.size * cuckooFilter.bucketSize
}

func (cuckooFilter CuckooFilter) Retries() uint64 {
	return cuckooFilter.retries
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

func (cuckooFilter CuckooFilter) CuckooPositiveRate() float64 {
	return math.Pow(2, math.Log2(float64(2*cuckooFilter.bucketSize))-float64(cuckooFilter.fingerPrintLength))
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

func (cuckooFilter CuckooFilter) getPositions(data []byte) (string, uint64, uint64, error) {
	hash := getHash(data)
	hashString := strconv.FormatUint(hash, 10)
	if cuckooFilter.fingerPrintLength > uint64(len(hashString)) {
		return "", 0, 0, fmt.Errorf("the fingerprint length %d is higher than the hash length %d", cuckooFilter.fingerPrintLength, len(hashString))
	}
	fingerPrint := hashString[:cuckooFilter.fingerPrintLength]
	firstIndex := hash % cuckooFilter.size
	secondHash := getHash([]byte(fingerPrint))
	secondIndex := (firstIndex ^ secondHash) % cuckooFilter.size
	return fingerPrint, firstIndex, secondIndex, nil
}

func getHash(data []byte) uint64 {
	hash1, _ := hash.Sum128(data)
	return hash1
}

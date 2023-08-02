package filters

import (
	"fmt"
	"math"
	"strconv"

	"github.com/gostatix/hash"
)

type BaseCuckooFilter interface {
	Size() uint64
	Length() uint64
	BucketSize() uint64
	FingerPrintLength() uint64
	CellSize() uint64
	Retries() uint64
}

type AbstractCuckooFilter struct {
	BaseCuckooFilter
	size              uint64
	bucketSize        uint64
	fingerPrintLength uint64
	length            uint64
	retries           uint64
}

func (cuckooFilter *AbstractCuckooFilter) Size() uint64 {
	return cuckooFilter.size
}

func (cuckooFilter *AbstractCuckooFilter) Length() uint64 {
	return cuckooFilter.length
}

func (cuckooFilter *AbstractCuckooFilter) BucketSize() uint64 {
	return cuckooFilter.bucketSize
}

func (cuckooFilter *AbstractCuckooFilter) FingerPrintLength() uint64 {
	return cuckooFilter.fingerPrintLength
}

func (cuckooFilter *AbstractCuckooFilter) CellSize() uint64 {
	return cuckooFilter.size * cuckooFilter.bucketSize
}

func (cuckooFilter *AbstractCuckooFilter) Retries() uint64 {
	return cuckooFilter.retries
}

func (cuckooFilter *AbstractCuckooFilter) getPositions(data []byte) (string, uint64, uint64, error) {
	hash := getHash(data)
	hashString := strconv.FormatUint(hash, 10)
	if cuckooFilter.fingerPrintLength > uint64(len(hashString)) {
		return "", 0, 0, fmt.Errorf("gostatix: the fingerprint length %d is higher than the hash length %d", cuckooFilter.fingerPrintLength, len(hashString))
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

func (cuckooFilter *AbstractCuckooFilter) CuckooPositiveRate() float64 {
	return math.Pow(2, math.Log2(float64(2*cuckooFilter.bucketSize))-float64(cuckooFilter.fingerPrintLength))
}

/*
Package filters provides data structures and methods for creating probabilistic filters.
This package provides implementations of two of the most widely used filters,
Bloom Filter and Cuckoo Filter.
*/
package gostatix

import (
	"fmt"
	"math"
	"strconv"
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
	retries           uint64
}

type entry struct {
	fingerPrint string
	firstIndex  uint64
	secondIndex uint64
}

func makeAbstractCuckooFilter(size, bucketSize, fingerPrintLength, retries uint64) *AbstractCuckooFilter {
	baseFilter := &AbstractCuckooFilter{}
	baseFilter.size = size
	baseFilter.bucketSize = bucketSize
	baseFilter.fingerPrintLength = fingerPrintLength
	baseFilter.retries = retries
	return baseFilter
}

// Size returns the size of the buckets slice of the Cuckoo Filter
func (cuckooFilter *AbstractCuckooFilter) Size() uint64 {
	return cuckooFilter.size
}

// BucketSize returns the size of the individual buckets of the Cuckoo Filter
func (cuckooFilter *AbstractCuckooFilter) BucketSize() uint64 {
	return cuckooFilter.bucketSize
}

// FingerPrintLength returns the length of the fingerprint of the Cuckoo Filter
func (cuckooFilter *AbstractCuckooFilter) FingerPrintLength() uint64 {
	return cuckooFilter.fingerPrintLength
}

// CellSize returns the overall size of the Cuckoo Filter - _size_ * _bucketSize_
func (cuckooFilter *AbstractCuckooFilter) CellSize() uint64 {
	return cuckooFilter.size * cuckooFilter.bucketSize
}

// Retries returns the number of retries that the Cuckoo Filter makes if the
// first and second indices returned after hashing the input is already occupied
// in the filter
func (cuckooFilter *AbstractCuckooFilter) Retries() uint64 {
	return cuckooFilter.retries
}

// CuckooPositiveRate returns the false positive error rate of the filter
func (cuckooFilter *AbstractCuckooFilter) CuckooPositiveRate() float64 {
	return math.Pow(2, math.Log2(float64(2*cuckooFilter.bucketSize))-float64(cuckooFilter.fingerPrintLength))
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
	hash1, _ := sum128(data)
	// hash := metro.Hash64(data, 1373)
	return hash1
}

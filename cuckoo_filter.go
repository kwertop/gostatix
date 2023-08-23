/*
Package filters provides data structures and methods for creating probabilistic filters.
This package provides implementations of two of the most widely used filters,
Bloom Filter and Cuckoo Filter.
*/
package gostatix

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"math/rand"
	"sync"

	"github.com/kwertop/gostatix/internal/util"
)

// CuckooFilter is the in-memory implementation of BaseCuckooFilter
// _buckets_ is a slice of BucketMem
// _length_ represents the number of entries present in the Cuckoo Filter
// _lock_ is used to synchronize concurrent read/writes
type CuckooFilter struct {
	buckets []BucketMem
	length  uint64
	*AbstractCuckooFilter
	lock sync.RWMutex
}

// NewCuckooFilter creates a new in-memory CuckooFilter
// _size_ is the size of the BucketMem slice
// _bucketSize_ is the size of the individual buckets inside the bucket slice
// _fingerPrintLength_ is fingerprint hash of the input to be inserted/removed/lookup
func NewCuckooFilter(size, bucketSize, fingerPrintLength uint64) *CuckooFilter {
	return NewCuckooFilterWithRetries(size, bucketSize, fingerPrintLength, 500)
}

// NewCuckooFilterWithRetries creates new in-memory CuckooFilter with specified _retries_
// _size_ is the size of the BucketMem slice
// _bucketSize_ is the size of the individual buckets inside the bucket slice
// _fingerPrintLength_ is fingerprint hash of the input to be inserted/removed/lookup
// _retries_ is the number of retries that the Cuckoo filter makes if the first two indices obtained
// after hashing the input is already occupied in the filter
func NewCuckooFilterWithRetries(size, bucketSize, fingerPrintLength, retries uint64) *CuckooFilter {
	filter := make([]BucketMem, size)
	for i := range filter {
		filter[i] = *NewBucketMem(bucketSize)
	}
	baseFilter := makeAbstractCuckooFilter(size, bucketSize, fingerPrintLength, retries)
	return &CuckooFilter{buckets: filter, AbstractCuckooFilter: baseFilter}
}

// NewCuckooFilterWithErrorRate creates an in-memory CuckooFilter with a specified false positive
// rate : _errorRate_
// _size_ is the size of the BucketMem slice
// _bucketSize_ is the size of the individual buckets inside the bucket slice
// _retries_ is the number of retries that the Cuckoo filter makes if the first two indices obtained
// _errorRate_ is the desired false positive rate of the filter. fingerPrintLength is calculated
// according to this error rate.
func NewCuckooFilterWithErrorRate(size, bucketSize, retries uint64, errorRate float64) *CuckooFilter {
	fingerPrintLength := util.CalculateFingerPrintLength(size, errorRate)
	capacity := uint64(math.Ceil(float64(size) * 0.955 / float64(bucketSize)))
	return NewCuckooFilterWithRetries(capacity, bucketSize, fingerPrintLength, retries)
}

// Length returns the current length of the Cuckoo Filter or the current number of entries
// present in the Cuckoo Filter
func (cuckooFilter *CuckooFilter) Length() uint64 {
	return cuckooFilter.length
}

// Insert writes the _data_ in the Cuckoo Filter for future lookup
// _destructive_ parameter is used to specify if the previous ordering of the
// present entries is to be preserved after the retries (if that case arises)
func (cuckooFilter *CuckooFilter) Insert(data []byte, destructive bool) bool {
	cuckooFilter.lock.Lock()
	defer cuckooFilter.lock.Unlock()

	fingerPrint, fIndex, sIndex, _ := cuckooFilter.getPositions(data)
	if cuckooFilter.buckets[fIndex].IsFree() {
		cuckooFilter.buckets[fIndex].Add(fingerPrint)
	} else if cuckooFilter.buckets[sIndex].IsFree() {
		cuckooFilter.buckets[sIndex].Add(fingerPrint)
	} else {
		var index uint64
		if rand.Float32() < 0.5 {
			index = fIndex
		} else {
			index = sIndex
		}
		currFingerPrint := fingerPrint
		var items []entry
		for i := uint64(0); i < cuckooFilter.retries; i++ {
			randIndex := uint64(math.Ceil(rand.Float64() * float64(cuckooFilter.buckets[index].Length()-1)))
			prevFingerPrint := cuckooFilter.buckets[index].At(randIndex)
			items = append(items, entry{prevFingerPrint, index, randIndex})
			cuckooFilter.buckets[index].Set(randIndex, currFingerPrint)
			hash := getHash([]byte(prevFingerPrint))
			newIndex := (index ^ hash) % uint64(len(cuckooFilter.buckets))
			if cuckooFilter.buckets[newIndex].IsFree() {
				cuckooFilter.buckets[newIndex].Add(prevFingerPrint)
				cuckooFilter.length++
				return true
			}
		}
		if !destructive {
			for i := len(items) - 1; i >= 0; i-- {
				item := items[i]
				cuckooFilter.buckets[item.firstIndex].Set(item.secondIndex, item.fingerPrint)
			}
		}
		panic("cannot insert element, cuckoofilter is full")
	}
	cuckooFilter.length++
	return true
}

// Lookup returns true if the _data_ is present in the Cuckoo Filter, else false
func (cuckooFilter *CuckooFilter) Lookup(data []byte) bool {
	cuckooFilter.lock.Lock()
	defer cuckooFilter.lock.Unlock()

	fingerPrint, fIndex, sIndex, _ := cuckooFilter.getPositions(data)
	return cuckooFilter.buckets[fIndex].Lookup(fingerPrint) ||
		cuckooFilter.buckets[sIndex].Lookup(fingerPrint)
}

// Remove deletes the _data_ from the Cuckoo Filter
func (cuckooFilter *CuckooFilter) Remove(data []byte) bool {
	cuckooFilter.lock.Lock()
	defer cuckooFilter.lock.Unlock()

	fingerPrint, fIndex, sIndex, _ := cuckooFilter.getPositions(data)
	if cuckooFilter.buckets[fIndex].Lookup(fingerPrint) {
		cuckooFilter.buckets[fIndex].Remove(fingerPrint)
		cuckooFilter.length--
		return true
	} else if cuckooFilter.buckets[sIndex].Lookup(fingerPrint) {
		cuckooFilter.buckets[sIndex].Remove(fingerPrint)
		cuckooFilter.length--
		return true
	} else {
		return false
	}
}

// Equals checks if two CuckooFilter are same or not
func (aFilter *CuckooFilter) Equals(bFilter *CuckooFilter) bool {
	count := 0
	result := true
	for result && count < len(aFilter.buckets) {
		bucket := aFilter.buckets[count]
		if !bFilter.buckets[count].Equals(&bucket) {
			return false
		}
		count++
	}
	return true
}

// bucketMemJSON is internal struct used to json marshal/unmarshal buckets
type bucketMemJSON struct {
	Size     uint64   `json:"s"`
	Length   uint64   `json:"l"`
	Elements []string `json:"e"`
}

// cuckooFilterMemJSON is internal struct used to json marshal/unmarshal cuckoo filter
type cuckooFilterMemJSON struct {
	Size              uint64          `json:"s"`
	BucketSize        uint64          `json:"bs"`
	FingerPrintLength uint64          `json:"fpl"`
	Length            uint64          `json:"l"`
	Retries           uint64          `json:"r"`
	Buckets           []bucketMemJSON `json:"b"`
}

// Export JSON marshals the CuckooFilter and returns a byte slice containing the data
func (cuckooFilter *CuckooFilter) Export() ([]byte, error) {
	bucketsJSON := make([]bucketMemJSON, cuckooFilter.size)
	for i := range cuckooFilter.buckets {
		bucket := cuckooFilter.buckets[i]
		bucketJSON := bucketMemJSON{bucket.Size(), bucket.Length(), bucket.Elements()}
		bucketsJSON[i] = bucketJSON
	}
	return json.Marshal(cuckooFilterMemJSON{
		cuckooFilter.size,
		cuckooFilter.bucketSize,
		cuckooFilter.fingerPrintLength,
		cuckooFilter.length,
		cuckooFilter.retries,
		bucketsJSON,
	})
}

// Import JSON unmarshals the _data_ into the CuckooFilter
func (cuckooFilter *CuckooFilter) Import(data []byte) error {
	var f cuckooFilterMemJSON
	err := json.Unmarshal(data, &f)
	if err != nil {
		return err
	}
	cuckooFilter.size = f.Size
	cuckooFilter.bucketSize = f.BucketSize
	cuckooFilter.fingerPrintLength = f.FingerPrintLength
	cuckooFilter.length = f.Length
	cuckooFilter.retries = f.Retries
	filters := make([]BucketMem, f.Size)
	for i := range f.Buckets {
		bucketJSON := f.Buckets[i]
		bucket := *NewBucketMem(f.BucketSize)
		for j := range bucketJSON.Elements {
			bucket.Add(bucketJSON.Elements[j])
		}
		filters[i] = bucket
	}
	cuckooFilter.buckets = filters
	return nil
}

// WriteTo writes the CuckooFilter onto the specified _stream_ and returns the
// number of bytes written.
// It can be used to write to disk (using a file stream) or to network.
func (cuckooFilter *CuckooFilter) WriteTo(stream io.Writer) (int64, error) {
	err := binary.Write(stream, binary.BigEndian, cuckooFilter.size)
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, cuckooFilter.bucketSize)
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, cuckooFilter.fingerPrintLength)
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, cuckooFilter.length)
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, cuckooFilter.retries)
	if err != nil {
		return 0, err
	}
	numBytes := int64(0)
	for i := uint64(0); i < cuckooFilter.size; i++ {
		bytes, err := cuckooFilter.buckets[i].WriteTo(stream)
		if err != nil {
			return 0, err
		}
		numBytes += bytes
	}
	return numBytes + int64(5*binary.Size(uint64(0))), nil
}

// ReadFrom reads the CuckooFilter from the specified _stream_ and returns the
// number of bytes read.
// It can be used to read from disk (using a file stream) or from network.
func (cuckooFilter *CuckooFilter) ReadFrom(stream io.Reader) (int64, error) {
	var size, bucketSize, fingerPrintLength, length, retries uint64
	err := binary.Read(stream, binary.BigEndian, &size)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &bucketSize)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &fingerPrintLength)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &length)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &retries)
	if err != nil {
		return 0, err
	}
	cuckooFilter.size = size
	cuckooFilter.bucketSize = bucketSize
	cuckooFilter.fingerPrintLength = fingerPrintLength
	cuckooFilter.length = length
	cuckooFilter.retries = retries
	cuckooFilter.buckets = make([]BucketMem, size)
	numBytes := int64(0)
	for i := uint64(0); i < cuckooFilter.size; i++ {
		bucket := NewBucketMem(0)
		bytes, err := bucket.ReadFrom(stream)
		if err != nil {
			return 0, err
		}
		numBytes += bytes
		cuckooFilter.buckets[i] = *bucket
	}
	return numBytes + int64(5*binary.Size(uint64(0))), nil
}

/*
Package buckets implements buckets - a container of fixed number of entries
used in cuckoo filters.
*/
package gostatix

import (
	"encoding/binary"
	"fmt"
	"io"
)

// BucketMem is in-memmory data structure holding the entries of the bucket
// used for cuckoo filters.
// _elements_ is the string slice which holds the actual values
// _length_ is used to track the number of non-empty/valied entries in the bucket
type BucketMem struct {
	elements []string
	length   uint64
	*AbstractBucket
}

// NewBucketMem creates a new BucketMem
func newBucketMem(size uint64) *BucketMem {
	bucket := &AbstractBucket{}
	bucket.size = size
	return &BucketMem{make([]string, size), 0, bucket}
}

// Length returns the number of entries in the bucket
func (bucket *BucketMem) getLength() uint64 {
	return bucket.length
}

// IsFree returns true if there is room for more entries in the bucket,
// otherwise false.
func (bucket *BucketMem) isFree() bool {
	return bucket.length < bucket.size
}

// Elements returns the string slice _elements_
func (bucket *BucketMem) getElements() []string {
	return bucket.elements
}

// NextSlot returns the next empty slot in the bucket starting from index 0
func (bucket *BucketMem) nextSlot() int64 {
	return bucket.indexOf("")
}

// At returns the value stored at _index_
func (bucket *BucketMem) at(index uint64) string {
	return bucket.elements[index]
}

// Add inserts the _element_ in the bucket at the next available slot
func (bucket *BucketMem) add(element string) bool {
	if element == "" || !bucket.isFree() {
		return false
	}
	bucket.set(uint64(bucket.nextSlot()), element)
	bucket.length++
	return true
}

// Remove deletes the entry _element_ from the bucket
func (bucket *BucketMem) remove(element string) bool {
	index := bucket.indexOf(element)
	if index <= -1 {
		return false
	}
	bucket.unSet(uint64(index))
	return true
}

// Lookup returns true if the _element_ is present in the bucket, otherwise false
func (bucket *BucketMem) lookup(element string) bool {
	return bucket.indexOf(element) > -1
}

// Set inserts the _element_ at the specified _index_
func (bucket *BucketMem) set(index uint64, element string) {
	bucket.elements[index] = element
}

// UnSet removes the element stored at the specified _index_
func (bucket *BucketMem) unSet(index uint64) {
	bucket.elements[index] = ""
	bucket.length--
}

// Swap inserts the specified _element_ at the specified _index_
// and returns the element stored previously stored at the _index_
func (bucket *BucketMem) swap(index uint64, element string) string {
	temp := bucket.elements[index]
	bucket.elements[index] = element
	return temp
}

// Equals checks if two BucketMem are equal
func (bucket *BucketMem) equals(otherBucket *BucketMem) bool {
	if bucket.size != otherBucket.size || bucket.length != otherBucket.length {
		return false
	}
	for index, val := range bucket.elements {
		if otherBucket.elements[index] != val {
			return false
		}
	}
	return true
}

// WriteTo writes the BucketMem onto the specified _stream_ and returns the
// number of bytes written.
// It can be used to write to disk (using a file stream) or to network.
func (bucket *BucketMem) writeTo(stream io.Writer) (int64, error) {
	err := binary.Write(stream, binary.BigEndian, uint64(bucket.size))
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, uint64(bucket.length))
	if err != nil {
		return 0, err
	}
	numBytes := 0
	for i := range bucket.elements {
		str := bucket.elements[i]
		err := binary.Write(stream, binary.BigEndian, uint64(len(str)))
		if err != nil {
			return 0, fmt.Errorf("gostatix: error encoding string length. error: %v", err)
		}
		bytes, err := stream.Write([]byte(str))
		if err != nil {
			return 0, fmt.Errorf("gostatix: error encoding string. error: %v", err)
		}
		numBytes += bytes
	}
	return int64(numBytes) + int64(2*binary.Size(uint64(0))), nil
}

// ReadFrom reads the BucketMem from the specified _stream_ and returns the
// number of bytes read.
// It can be used to read from disk (using a file stream) or from network.
func (bucket *BucketMem) readFrom(stream io.Reader) (int64, error) {
	var size, length uint64
	err := binary.Read(stream, binary.BigEndian, &size)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &length)
	if err != nil {
		return 0, err
	}
	bucket.size = size
	bucket.length = length
	bucket.elements = make([]string, size)
	numBytes := 0
	for i := uint64(0); i < length; i++ {
		var strLen uint64
		err := binary.Read(stream, binary.BigEndian, &strLen)
		if err != nil {
			return 0, err
		}
		b := make([]byte, strLen)
		bytes, err := io.ReadFull(stream, b)
		if err != nil {
			return 0, err
		}
		numBytes += bytes
		bucket.elements[i] = string(b)
	}
	return int64(numBytes) + int64(2*binary.Size(uint64(0))), nil
}

func (bucket BucketMem) indexOf(element string) int64 {
	for index, val := range bucket.elements {
		if val == element {
			return int64(index)
		}
	}
	return int64(-1)
}

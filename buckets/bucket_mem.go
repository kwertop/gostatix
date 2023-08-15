package buckets

import (
	"encoding/binary"
	"fmt"
	"io"
)

type BucketMem struct {
	elements []string
	*AbstractBucket
}

func NewBucketMem(size uint64) *BucketMem {
	bucket := &AbstractBucket{}
	bucket.size = size
	bucket.length = 0
	return &BucketMem{make([]string, size), bucket}
}

func (bucket BucketMem) Elements() []string {
	return bucket.elements
}

func (bucket BucketMem) NextSlot() int64 {
	return bucket.indexOf("")
}

func (bucket BucketMem) At(index uint64) string {
	return bucket.elements[index]
}

func (bucket *BucketMem) Add(element string) bool {
	if element == "" || !bucket.IsFree() {
		return false
	}
	bucket.Set(uint64(bucket.NextSlot()), element)
	bucket.length++
	return true
}

func (bucket BucketMem) Remove(element string) bool {
	index := bucket.indexOf(element)
	if index <= -1 {
		return false
	}
	bucket.UnSet(uint64(index))
	return true
}

func (bucket BucketMem) Lookup(element string) bool {
	return bucket.indexOf(element) > -1
}

func (bucket BucketMem) Set(index uint64, element string) {
	bucket.elements[index] = element
}

func (bucket *BucketMem) UnSet(index uint64) {
	bucket.elements[index] = ""
	bucket.length--
}

func (bucket *BucketMem) Swap(index uint64, element string) string {
	temp := bucket.elements[index]
	bucket.elements[index] = element
	return temp
}

func (bucket BucketMem) Equals(otherBucket BucketMem) bool {
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

func (bucket *BucketMem) WriteTo(stream io.Writer) (int64, error) {
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

func (bucket *BucketMem) ReadFrom(stream io.Reader) (int64, error) {
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
	for i := uint64(0); i < size; i++ {
		var strLen uint64
		err := binary.Read(stream, binary.BigEndian, &size)
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

/*
Package bitset implements bitsets - both in-memory and redis.
For in-memory, https://github.com/bits-and-blooms/bitset is used while
for redis, bitset operations of redis are used.
*/
package gostatix

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/bits-and-blooms/bitset"
)

// BitSetMem is an implementation of IBitSet.
// _size_ is the number of bits in the bitset
// _set_ is the bitset implementation adopted from https://github.com/bits-and-blooms/bitset
type BitSetMem struct {
	set  *bitset.BitSet
	size uint
}

// NewBitSetMem creates a new BitSetMem of size _size_
func newBitSetMem(size uint) *BitSetMem {
	return &BitSetMem{bitset.New(size), size}
}

// FromDataMem creates an instance of BitSetMem after
// inserting the data passed in the bitset
func fromDataMem(data []uint64) *BitSetMem {
	return &BitSetMem{bitset.From(data), uint(len(data) * 64)}
}

// Size returns the size of the bitset
func (bitSet BitSetMem) Size() uint {
	return bitSet.size
}

// Has checks if the bit at index _index_ is set
func (bitSet BitSetMem) Has(index uint) (bool, error) {
	return bitSet.set.Test(index), nil
}

// HasMulti checks if the bit at the indices
// specified by _indexes_ array is set
func (bitSet BitSetMem) HasMulti(indexes []uint) ([]bool, error) {
	return nil, nil //not implemented
}

func (bitSet BitSetMem) InsertMulti(indexes []uint) (bool, error) {
	return false, nil //not implemented
}

// Insert sets the bit at index specified by _index_
func (bitSet BitSetMem) Insert(index uint) (bool, error) {
	bitSet.set.Set(index)
	return true, nil
}

// Max returns the first set bit in the bitset starting from index 0
func (bitSet BitSetMem) Max() (uint, bool) {
	index, ok := bitSet.set.NextSet(0)
	return index, ok
}

// BitCount returns the total number of set bits in the bitset
func (bitSet BitSetMem) BitCount() (uint, error) {
	return bitSet.set.Count(), nil
}

// Export returns the json marshalling of the bitset
func (bitSet BitSetMem) Export() (uint, []byte, error) {
	data, err := bitSet.set.MarshalJSON()
	if err != nil {
		return 0, nil, err
	}
	return bitSet.size, data, nil
}

// ExportBinary returns the binary marshalling of the bitset
func (bitSet BitSetMem) ExportBinary() (uint, []byte, error) {
	data, err := bitSet.set.MarshalBinary()
	if err != nil {
		return 0, nil, err
	}
	return bitSet.size, data, nil
}

// Import imports the marshalled json in the byte array data into the redis bitset
func (bitSet *BitSetMem) Import(data []byte) (bool, error) {
	err := bitSet.set.UnmarshalJSON(data)
	bitSet.size = bitSet.set.Len()
	if err != nil {
		return false, err
	}
	return true, nil
}

// Equals checks if two BitSetMem are equal or not
func (firstBitSet *BitSetMem) Equals(otherBitSet IBitSet) (bool, error) {
	secondBitSet, ok := otherBitSet.(*BitSetMem)
	if !ok {
		return false, fmt.Errorf("invalid bitset type, should be BitSetMem, type: %v", secondBitSet)
	}
	return firstBitSet.set.Equal(secondBitSet.set), nil
}

// WriteTo writes the bitset to a stream and returns the number of bytes written onto the stream
func (bitSet *BitSetMem) WriteTo(stream io.Writer) (int64, error) {
	err := binary.Write(stream, binary.BigEndian, uint64(bitSet.size))
	if err != nil {
		return 0, err
	}
	numBytes, err := bitSet.set.WriteTo(stream)
	if err != nil {
		return 0, err
	}
	return numBytes + int64(binary.Size(uint64(0))), nil
}

// ReadFrom reads the stream and imports it into the bitset and returns the number of bytes read
func (bitSet *BitSetMem) ReadFrom(stream io.Reader) (int64, error) {
	var size uint64
	err := binary.Read(stream, binary.BigEndian, &size)
	if err != nil {
		return 0, err
	}
	set := &bitset.BitSet{}
	numBytes, err := set.ReadFrom(stream)
	if err != nil {
		return 0, err
	}
	bitSet.size = uint(size)
	bitSet.set = set
	return numBytes + int64(binary.Size(uint64(0))), nil
}

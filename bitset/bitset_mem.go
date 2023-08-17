package bitset

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/bits-and-blooms/bitset"
)

type BitSetMem struct {
	set  *bitset.BitSet
	size uint
}

func NewBitSetMem(size uint) *BitSetMem {
	return &BitSetMem{bitset.New(size), size}
}

func FromDataMem(data []uint64) *BitSetMem {
	return &BitSetMem{bitset.From(data), uint(len(data) * 64)}
}

func (bitSet BitSetMem) Size() uint {
	return bitSet.size
}

func (bitSet BitSetMem) Has(index uint) (bool, error) {
	return bitSet.set.Test(index), nil
}

func (bitSet BitSetMem) HasMulti(indexes []uint) ([]bool, error) {
	return nil, nil //not implemented
}

func (bitSet BitSetMem) InsertMulti(indexes []uint) (bool, error) {
	return false, nil //not implemented
}

func (bitSet BitSetMem) Insert(index uint) (bool, error) {
	bitSet.set.Set(index)
	return true, nil
}

func (bitSet BitSetMem) Max() (uint, bool) {
	index, ok := bitSet.set.NextSet(0)
	return index, ok
}

func (bitSet BitSetMem) BitCount() (uint, error) {
	return bitSet.set.Count(), nil
}

func (bitSet BitSetMem) Export() (uint, []byte, error) {
	data, err := bitSet.set.MarshalJSON()
	if err != nil {
		return 0, nil, err
	}
	return bitSet.size, data, nil
}

func (bitSet BitSetMem) ExportBinary() (uint, []byte, error) {
	data, err := bitSet.set.MarshalBinary()
	if err != nil {
		return 0, nil, err
	}
	return bitSet.size, data, nil
}

func (bitSet *BitSetMem) Import(data []byte) (bool, error) {
	err := bitSet.set.UnmarshalJSON(data)
	bitSet.size = bitSet.set.Len()
	if err != nil {
		return false, err
	}
	return true, nil
}

func (firstBitSet BitSetMem) Equals(otherBitSet IBitSet) (bool, error) {
	secondBitSet, ok := otherBitSet.(*BitSetMem)
	if !ok {
		return false, fmt.Errorf("invalid bitset type, should be BitSetMem, type: %v", secondBitSet)
	}
	return firstBitSet.set.Equal(secondBitSet.set), nil
}

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

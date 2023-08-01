package bitset

import (
	"fmt"

	"github.com/bits-and-blooms/bitset"
)

type BitSetMem struct {
	set  bitset.BitSet
	size uint
}

func NewBitSetMem(size uint) *BitSetMem {
	return &BitSetMem{*bitset.New(size), size}
}

func FromDataMem(data []uint64) *BitSetMem {
	return &BitSetMem{*bitset.From(data), uint(len(data))}
}

func (bitSet BitSetMem) Has(index uint) (bool, error) {
	return bitSet.set.Test(index), nil
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

func (bitSet BitSetMem) Import(size uint, data []byte) (bool, error) {
	err := bitSet.set.UnmarshalJSON(data)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (firstBitSet BitSetMem) Equals(otherBitSet IBitSet) (bool, error) {
	secondBitSet, ok := otherBitSet.(BitSetMem)
	if !ok {
		return false, fmt.Errorf("invalid bitset type, should be BitSetMem")
	}
	return firstBitSet.set.Equal(&secondBitSet.set), nil
}

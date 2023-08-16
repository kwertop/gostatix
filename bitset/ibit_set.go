package bitset

import "io"

const wordSize = uint(64)
const wordBytes = wordSize / 8

type IBitSet interface {
	Size() uint
	Has(index uint) (bool, error)
	Insert(index uint) (bool, error)
	Equals(otherBitSet IBitSet) (bool, error)
	Max() (uint, bool)
	BitCount() (uint, error)
	Export() (uint, []byte, error)
	Import(data []byte) (bool, error)
	WriteTo(stream io.Writer) (int64, error)
	ReadFrom(stream io.Reader) (int64, error)
}

func IsBitSetMem(t interface{}) bool {
	switch t.(type) {
	case *BitSetMem:
		return true
	default:
		return false
	}
}

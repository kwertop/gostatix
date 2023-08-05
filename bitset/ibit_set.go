package bitset

const wordSize = uint(64)
const wordBytes = wordSize / 8

type IBitSet interface {
	Has(index uint) (bool, error)
	Insert(index uint) (bool, error)
	Equals(otherBitSet IBitSet) (bool, error)
	Max() (uint, bool)
	BitCount() (uint, error)
	Export() (uint, []byte, error)
	Import(size uint, data []byte) (bool, error)
}

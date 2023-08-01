package bitset

type IBitSet interface {
	Has(index uint) (bool, error)
	Insert(index uint) (bool, error)
	Equals(otherBitSet IBitSet) (bool, error)
	Max() (uint, bool)
	BitCount() (uint, error)
	Export() (uint, []byte, error)
	Import(size uint, data []byte) (bool, error)
}

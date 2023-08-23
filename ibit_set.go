/*
Package bitset implements bitsets - both in-memory and redis.
For in-memory, https://github.com/bits-and-blooms/bitset is used while
for redis, bitset operations of redis are used.
*/
package gostatix

import "io"

const wordSize = int(64)
const wordBytes = wordSize / 8

type IBitSet interface {
	// Size returns the number of bits in the bitset
	Size() uint

	// Has returns true if the bit is set at index, else false
	Has(index uint) (bool, error)

	// HasMulti returns an array of boolean values for the queried
	// index values in the indexes array
	HasMulti(indexes []uint) ([]bool, error)

	// Insert sets the bit at index to true
	Insert(index uint) (bool, error)

	// Insert sets the bits at the indices passed in the indexes array
	InsertMulti(indexes []uint) (bool, error)

	// Equals checks if two bitsets are equal
	Equals(otherBitSet IBitSet) (bool, error)

	// Max returns the first set bit in the bitset
	// starting from index 0
	Max() (uint, bool)

	// BitCount returns the total number of set bits in the bitset
	BitCount() (uint, error)

	// Export returns the json marshalling of the bitset
	Export() (uint, []byte, error)

	// Import imports the byte array data into the bitset
	Import(data []byte) (bool, error)

	// WriteTo writes the bitset to a stream and
	// returns the number of bytes written onto the stream
	WriteTo(stream io.Writer) (int64, error)

	// ReadFrom reads the stream and imports it into the bitset
	// and returns the number of bytes read
	ReadFrom(stream io.Reader) (int64, error)
}

// Function IsBitSetMem is used to check if the passed variable `t`
// is of type *BitSetMem or not
func IsBitSetMem(t interface{}) bool {
	switch t.(type) {
	case *BitSetMem:
		return true
	default:
		return false
	}
}

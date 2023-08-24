/*
Implements bitsets - both in-memory and redis.
For in-memory, https://github.com/bits-and-blooms/bitset is used while
for redis, bitset operations of redis are used.
*/
package gostatix

import "io"

const wordSize = int(64)
const wordBytes = wordSize / 8

type IBitSet interface {
	// Size returns the number of bits in the bitset
	getSize() uint

	// Has returns true if the bit is set at index, else false
	has(index uint) (bool, error)

	// HasMulti returns an array of boolean values for the queried
	// index values in the indexes array
	hasMulti(indexes []uint) ([]bool, error)

	// Insert sets the bit at index to true
	insert(index uint) (bool, error)

	// Insert sets the bits at the indices passed in the indexes array
	insertMulti(indexes []uint) (bool, error)

	// Equals checks if two bitsets are equal
	equals(otherBitSet IBitSet) (bool, error)

	// Max returns the first set bit in the bitset
	// starting from index 0
	max() (uint, bool)

	// BitCount returns the total number of set bits in the bitset
	bitCount() (uint, error)

	// Export returns the json marshalling of the bitset
	marshal() (uint, []byte, error)

	// Import imports the byte array data into the bitset
	unmarshal(data []byte) (bool, error)

	// WriteTo writes the bitset to a stream and
	// returns the number of bytes written onto the stream
	writeTo(stream io.Writer) (int64, error)

	// ReadFrom reads the stream and imports it into the bitset
	// and returns the number of bytes read
	readFrom(stream io.Reader) (int64, error)
}

// Function IsBitSetMem is used to check if the passed variable `t`
// is of type *BitSetMem or not
func isBitSetMem(t interface{}) bool {
	switch t.(type) {
	case *BitSetMem:
		return true
	default:
		return false
	}
}

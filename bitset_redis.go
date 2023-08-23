/*
Package bitset implements bitsets - both in-memory and redis.
For in-memory, https://github.com/bits-and-blooms/bitset is used while
for redis, bitset operations of redis are used.
*/
package gostatix

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"encoding/base64"
	"encoding/binary"
	"encoding/json"

	"github.com/kwertop/gostatix/internal/util"
	"github.com/redis/go-redis/v9"
)

// BitSetRedis is an implementation of IBitSet.
// size is the number of bits in the bitset
// key is the redis key to the bitset data structure in redis
// Bitsets or Bitmaps are implemented in Redis using string.
// All bit operations are done on the string stored at _key_.
// For more details, please refer https://redis.io/docs/data-types/bitmaps/
type BitSetRedis struct {
	size uint
	key  string
}

// NewBitSetRedis creates a new BitSetRedis of size _size_
func newBitSetRedis(size uint) *BitSetRedis {
	bytes := make([]byte, size)
	for i := range bytes {
		bytes[i] = 0x00
	}
	key := util.GenerateRandomString(16)
	_ = getRedisClient().Set(context.Background(), key, string(bytes), 0).Err()
	return &BitSetRedis{size, key}
}

// FromDataRedis creates an instance of BitSetRedis after
// inserting the data passed in a redis bitset
func fromDataRedis(data []uint64) (*BitSetRedis, error) {
	bitSetRedis := newBitSetRedis(uint(len(data) * wordSize))
	bytes, err := uint64ArrayToByteArray(data)
	if err != nil {
		return nil, err
	}
	err = getRedisClient().Set(context.Background(), bitSetRedis.key, string(bytes), 0).Err()
	if err != nil {
		return nil, err
	}
	return bitSetRedis, nil
}

// FromRedisKey creates an instance of BitSetRedis from the
// bitset data structure saved at redis key _key_
func fromRedisKey(key string) (*BitSetRedis, error) {
	setVal, err := getRedisClient().Get(context.Background(), key).Result()
	if err != nil {
		return nil, err
	}
	setValBytes := []byte(setVal)
	bitSetRedis := newBitSetRedis(uint(len(setValBytes) * wordBytes))
	bitSetRedis.key = key
	return bitSetRedis, nil
}

// Size returns the size of the bitset saved in redis
func (bitSet BitSetRedis) getSize() uint {
	return bitSet.size
}

// Key gives the key at which the bitset is saved in redis
func (bitSet BitSetRedis) getKey() string {
	return bitSet.key
}

// Has checks if the bit at index _index_ is set
func (bitSet BitSetRedis) has(index uint) (bool, error) {
	val, err := getRedisClient().GetBit(context.Background(), bitSet.key, int64(index)).Result()
	if err != nil {
		return false, err
	}
	return val != 0, nil
}

// HasMulti checks if the bit at the indices
// specified by _indexes_ array is set
func (bitSet BitSetRedis) hasMulti(indexes []uint) ([]bool, error) {
	if len(indexes) == 0 {
		return nil, fmt.Errorf("gostatix: at least 1 index is required")
	}
	pipe := getRedisClient().Pipeline()
	ctx := context.Background()
	values := make([]*redis.IntCmd, len(indexes))
	for i := range indexes {
		values[i] = pipe.GetBit(ctx, bitSet.key, int64(indexes[i]))
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]bool, len(values))
	for i := range values {
		result[i] = values[i].Val() != 0
	}
	return result, nil
}

// Insert sets the bit at index specified by _index_
func (bitSet BitSetRedis) insert(index uint) (bool, error) {
	err := getRedisClient().SetBit(context.Background(), bitSet.key, int64(index), 1).Err()
	if err != nil {
		return false, err
	}
	return true, nil
}

// Insert sets the bits at indices specified by array _indexes_
func (bitSet BitSetRedis) insertMulti(indexes []uint) (bool, error) {
	if len(indexes) == 0 {
		return false, fmt.Errorf("gostatix: at least 1 index is required")
	}
	pipe := getRedisClient().Pipeline()
	ctx := context.Background()
	for i := range indexes {
		pipe.SetBit(ctx, bitSet.key, int64(indexes[i]), 1)
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Equals checks if two BitSetRedis are equal or not
func (aSet BitSetRedis) equals(otherBitSet IBitSet) (bool, error) {
	bSet, ok := otherBitSet.(*BitSetRedis)
	if !ok {
		return false, fmt.Errorf("invalid bitset type, should be BitSetRedis")
	}
	aSetVal, err1 := getRedisClient().Get(context.Background(), aSet.key).Result()
	if err1 != nil {
		return false, err1
	}
	bSetVal, err2 := getRedisClient().Get(context.Background(), bSet.key).Result()
	if err2 != nil {
		return false, err2
	}
	return aSetVal == bSetVal, nil
}

// Max returns the first set bit in the bitset starting from index 0
func (bitSet BitSetRedis) max() (uint, bool) {
	index, err := getRedisClient().BitPos(context.Background(), bitSet.key, 1).Result()
	if err != nil || index == -1 {
		return 0, false
	}
	return uint(index), true
}

// BitCount returns the total number of set bits in the bitset saved in redis
func (bitSet BitSetRedis) bitCount() (uint, error) {
	bitRange := &redis.BitCount{Start: 0, End: -1}
	val, err := getRedisClient().BitCount(context.Background(), bitSet.key, bitRange).Result()
	if err != nil {
		return 0, err
	}
	return uint(val), nil
}

// Export returns the json marshalling of the bitset saved in redis
func (bitSet BitSetRedis) marshal() (uint, []byte, error) {
	val, err := getRedisClient().Get(context.Background(), bitSet.key).Result()
	if err != nil {
		return 0, nil, err
	}
	bytes := []byte(val)
	for i := range bytes {
		bytes[i] = util.ConvertByteToLittleEndianByte(bytes[i])
	}
	util.ReverseBytes(bytes)
	buf := make([]byte, wordBytes)
	binary.BigEndian.PutUint64(buf, uint64(bitSet.size))
	bytes = append(buf, bytes...)
	data, err := json.Marshal(base64.URLEncoding.EncodeToString([]byte(bytes)))
	if err != nil {
		return 0, nil, err
	}
	return bitSet.size, data, nil
}

// Import imports the marshalled json in the byte array data into the redis bitset
func (bitSet *BitSetRedis) unmarshal(data []byte) (bool, error) {
	var s string
	err := json.Unmarshal(data, &s)
	if err != nil {
		return false, err
	}
	bytes, _ := base64.URLEncoding.DecodeString(s)
	lenBytes := bytes[:8]
	bytes = bytes[8:]
	size := binary.BigEndian.Uint64(lenBytes)
	bitSet.size = uint(size)
	util.ReverseBytes(bytes)
	for i := range bytes {
		bytes[i] = util.ConvertByteToLittleEndianByte(bytes[i])
	}
	err = getRedisClient().Set(context.Background(), bitSet.key, string(bytes), 0).Err()
	if err != nil {
		return false, err
	}
	return true, nil
}

func uint64ArrayToByteArray(data []uint64) ([]byte, error) {
	// Create a buffer to store the bytes
	buf := new(bytes.Buffer)

	// Write each uint64 element to the buffer
	for _, value := range data {
		valueBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(valueBytes, value)
		for _, val := range valueBytes {
			if err := binary.Write(buf, binary.LittleEndian, util.ConvertByteToLittleEndianByte(val)); err != nil {
				return nil, err
			}
		}
	}

	return buf.Bytes(), nil
}

func (bitSet *BitSetRedis) writeTo(stream io.Writer) (int64, error) {
	return 0, nil //bitsetredis doesn't implement WriteTo function
}

func (bitSet *BitSetRedis) readFrom(stream io.Reader) (int64, error) {
	return 0, nil //bitsetredis doesn't implement ReadFrom function
}

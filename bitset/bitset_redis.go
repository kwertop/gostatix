package bitset

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"encoding/base64"
	"encoding/binary"
	"encoding/json"

	"github.com/kwertop/gostatix"
	"github.com/redis/go-redis/v9"
)

type BitSetRedis struct {
	size uint
	key  string
}

func NewBitSetRedis(size uint) *BitSetRedis {
	bytes := make([]byte, size)
	for i := range bytes {
		bytes[i] = 0x00
	}
	key := gostatix.GenerateRandomString(16)
	_ = gostatix.GetRedisClient().Set(context.Background(), key, string(bytes), 0).Err()
	return &BitSetRedis{size, key}
}

func FromDataRedis(data []uint64) (*BitSetRedis, error) {
	bitSetRedis := NewBitSetRedis(uint(len(data) * wordSize))
	bytes, err := uint64ArrayToByteArray(data)
	if err != nil {
		return nil, err
	}
	err = gostatix.GetRedisClient().Set(context.Background(), bitSetRedis.key, string(bytes), 0).Err()
	if err != nil {
		return nil, err
	}
	return bitSetRedis, nil
}

func FromRedisKey(key string) (*BitSetRedis, error) {
	setVal, err := gostatix.GetRedisClient().Get(context.Background(), key).Result()
	if err != nil {
		return nil, err
	}
	setValBytes := []byte(setVal)
	bitSetRedis := NewBitSetRedis(uint(len(setValBytes) * wordBytes))
	bitSetRedis.key = key
	return bitSetRedis, nil
}

func (bitSet BitSetRedis) Size() uint {
	return bitSet.size
}

func (bitSet BitSetRedis) Key() string {
	return bitSet.key
}

func (bitSet BitSetRedis) Has(index uint) (bool, error) {
	val, err := gostatix.GetRedisClient().GetBit(context.Background(), bitSet.key, int64(index)).Result()
	if err != nil {
		return false, err
	}
	return val != 0, nil
}

func (bitSet BitSetRedis) HasMulti(indexes []uint) ([]bool, error) {
	if len(indexes) == 0 {
		return nil, fmt.Errorf("gostatix: at least 1 index is required")
	}
	pipe := gostatix.GetRedisClient().Pipeline()
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

func (bitSet BitSetRedis) Insert(index uint) (bool, error) {
	err := gostatix.GetRedisClient().SetBit(context.Background(), bitSet.key, int64(index), 1).Err()
	if err != nil {
		return false, err
	}
	return true, nil
}

func (bitSet BitSetRedis) InsertMulti(indexes []uint) (bool, error) {
	if len(indexes) == 0 {
		return false, fmt.Errorf("gostatix: at least 1 index is required")
	}
	pipe := gostatix.GetRedisClient().Pipeline()
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

func (aSet BitSetRedis) Equals(otherBitSet IBitSet) (bool, error) {
	bSet, ok := otherBitSet.(*BitSetRedis)
	if !ok {
		return false, fmt.Errorf("invalid bitset type, should be BitSetRedis")
	}
	aSetVal, err1 := gostatix.GetRedisClient().Get(context.Background(), aSet.key).Result()
	if err1 != nil {
		return false, err1
	}
	bSetVal, err2 := gostatix.GetRedisClient().Get(context.Background(), bSet.key).Result()
	if err2 != nil {
		return false, err2
	}
	return aSetVal == bSetVal, nil
}

func (bitSet BitSetRedis) Max() (uint, bool) {
	index, err := gostatix.GetRedisClient().BitPos(context.Background(), bitSet.key, 1).Result()
	if err != nil || index == -1 {
		return 0, false
	}
	return uint(index), true
}

func (bitSet BitSetRedis) BitCount() (uint, error) {
	bitRange := &redis.BitCount{Start: 0, End: -1}
	val, err := gostatix.GetRedisClient().BitCount(context.Background(), bitSet.key, bitRange).Result()
	if err != nil {
		return 0, err
	}
	return uint(val), nil
}

func (bitSet BitSetRedis) Export() (uint, []byte, error) {
	val, err := gostatix.GetRedisClient().Get(context.Background(), bitSet.key).Result()
	if err != nil {
		return 0, nil, err
	}
	bytes := []byte(val)
	for i := range bytes {
		bytes[i] = gostatix.ConvertByteToLittleEndianByte(bytes[i])
	}
	gostatix.ReverseBytes(bytes)
	buf := make([]byte, wordBytes)
	binary.BigEndian.PutUint64(buf, uint64(bitSet.size))
	bytes = append(buf, bytes...)
	data, err := json.Marshal(base64.URLEncoding.EncodeToString([]byte(bytes)))
	if err != nil {
		return 0, nil, err
	}
	return bitSet.size, data, nil
}

func (bitSet *BitSetRedis) Import(data []byte) (bool, error) {
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
	gostatix.ReverseBytes(bytes)
	for i := range bytes {
		bytes[i] = gostatix.ConvertByteToLittleEndianByte(bytes[i])
	}
	err = gostatix.GetRedisClient().Set(context.Background(), bitSet.key, string(bytes), 0).Err()
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
			if err := binary.Write(buf, binary.LittleEndian, gostatix.ConvertByteToLittleEndianByte(val)); err != nil {
				return nil, err
			}
		}
	}

	return buf.Bytes(), nil
}

func (bitSet *BitSetRedis) WriteTo(stream io.Writer) (int64, error) {
	return 0, nil //bitsetredis doesn't implement WriteTo function
}

func (bitSet *BitSetRedis) ReadFrom(stream io.Reader) (int64, error) {
	return 0, nil //bitsetredis doesn't implement ReadFrom function
}

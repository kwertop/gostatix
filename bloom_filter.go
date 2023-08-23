/*
Package filters provides data structures and methods for creating probabilistic filters.
This package provides implementations of two of the most widely used filters,
Bloom Filter and Cuckoo Filter.
*/
package gostatix

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"

	"github.com/dgryski/go-metro"
	"github.com/kwertop/gostatix/internal/util"
)

// The BloomFilter data structure. It mainly has two fields: _size_ and _numHashes_
// _size_ denotes the maximum size of the bloom filter
// _numHashes_ denotes the number of hashing functions applied on the entrant element
// during insertion or lookup.
// _filter_ is the bitset backing internally the bloom filter. It can either be a type of
// BitSetMem (in-memory) or BitSetRedis (redis-backed).
// _metadataKey_ saves the information about a Bloom Filter saved on Redis
// _lock_ is used to synchronize read/write on an in-memory BitSetMem. It's not used for
// BitSetRedis as Redis is event-driven single threaded
type BloomFilter struct {
	size        uint
	numHashes   uint
	filter      IBitSet
	metadataKey string
	lock        sync.RWMutex
}

// NewBloomFilterWithBitSet creates and returns a new BloomFilter
// _size_ is the maximum size of the bloom filter
// _numHashes_ is the number of hashing functions to be applied on the entrant
// _filter_ is either BitSetMem or BitSetRedis
// _metadataKey_ is needed if the filter is of type BitSetRedis otherwise it's overlooked
func NewBloomFilterWithBitSet(size, numHashes uint, filter IBitSet, metadataKey string) (*BloomFilter, error) {
	if !isBitSetMem(filter) && metadataKey == "" {
		return nil, fmt.Errorf("gostatix: error initializing filter as metadataKey is blank for BitSetRedis")
	}
	if filter.getSize() != size {
		return nil, fmt.Errorf("gostatix: error initializing filter as size of bitset %v doesn't match with size %v passed", filter.getSize(), size)
	}
	return &BloomFilter{
		size:        util.Max(size, 1),
		numHashes:   util.Max(numHashes, 1),
		filter:      filter,
		metadataKey: metadataKey,
	}, nil
}

// NewRedisBloomFilterWithParameters creates and returns a new Redis backed BloomFilter
// _numItems_ is the number of items for which the bloom filter has to be checked for validation
// _errorRate_ is the acceptable false positive error rate
// Based upon the above two parameters passed, the size of the bloom filter is calculated
// metadataKey is created using a random alpha-numeric generator which can be retrieved using
// MetadataKey() method
func NewRedisBloomFilterWithParameters(numItems uint, errorRate float64) (*BloomFilter, error) {
	size := util.CalculateFilterSize(numItems, errorRate)
	numHashes := util.CalculateNumHashes(size, numItems)
	filter := newBitSetRedis(size)
	metadataKey := util.GenerateRandomString(16)
	metadata := make(map[string]interface{})
	metadata["size"] = size
	metadata["numHashes"] = numHashes
	metadata["bitsetKey"] = filter.getKey()
	err := getRedisClient().HSet(context.Background(), metadataKey, metadata).Err()
	if err != nil {
		return nil, fmt.Errorf("gostatix: error while creating bloom filter redis. error: %v", err)
	}
	return NewBloomFilterWithBitSet(size, numHashes, filter, metadataKey)
}

// NewRedisBloomFilterWithParameters creates and returns a new in-memory BloomFilter
// _numItems_ is the number of items for which the bloom filter has to be checked for validation
// _errorRate_ is the acceptable false positive error rate
// Based upon the above two parameters passed, the size of the bloom filter is calculated
func NewMemBloomFilterWithParameters(numItems uint, errorRate float64) (*BloomFilter, error) {
	size := util.CalculateFilterSize(numItems, errorRate)
	numHashes := util.CalculateNumHashes(size, numItems)
	filter := newBitSetMem(size)
	return NewBloomFilterWithBitSet(util.Max(size, 1), util.Max(numHashes, 1), filter, "")
}

// NewRedisBloomFilterFromBitSet creates and returns a new Redis backed BloomFilter from the
// bitset passed in the parameter _data_
// _numHashes_ parameter is needed for the number of hashing functions
func NewRedisBloomFilterFromBitSet(data []uint64, numHashes uint) (*BloomFilter, error) {
	size := util.Max(uint(len(data)*64), 1)
	numHashes = util.Max(numHashes, 1)
	metadataKey := util.GenerateRandomString(16)
	err := getRedisClient().HSet(context.Background(), metadataKey, map[string]interface{}{"size": size, "numHashes": numHashes}).Err()
	if err != nil {
		return nil, fmt.Errorf("gostatix: error while creating bloom filter redis. error: %v", err)
	}
	bitSetRedis, err := fromDataRedis(data)
	if err != nil {
		return nil, err
	}
	return &BloomFilter{
		size:      size,
		numHashes: numHashes,
		filter:    bitSetRedis,
	}, nil
}

// NewRedisBloomFilterFromBitSet creates and returns a new in-memory BloomFilter from the
// bitset passed in the parameter _data_
// _numHashes_ parameter is needed for the number of hashing functions
func NewMemBloomFilterFromBitSet(data []uint64, numHashes uint) *BloomFilter {
	size := uint(len(data) * 64)
	return &BloomFilter{size: util.Max(size, 1), numHashes: util.Max(numHashes, 1), filter: fromDataMem(data)}
}

// NewRedisBloomFilterFromKey is used to create a new Redis backed BloomFilter from the
// _metadataKey_ (the Redis key used to store the metadata about the bloom filter) passed
// For this to work, value should be present in Redis at _key_
func NewRedisBloomFilterFromKey(metadataKey string) (*BloomFilter, error) {
	values, err := getRedisClient().HGetAll(context.Background(), metadataKey).Result()
	if err != nil {
		return nil, fmt.Errorf("gostatix: error while fetching hash from redis, error: %v", err)
	}
	bloomFilter := &BloomFilter{}
	size, _ := strconv.Atoi(values["size"])
	numHashes, _ := strconv.Atoi(values["numHashes"])
	bloomFilter.size = uint(size)
	bloomFilter.numHashes = uint(numHashes)
	bloomFilter.metadataKey = metadataKey
	bitsetKey := values["bitsetKey"]
	filter, _ := fromRedisKey(bitsetKey)
	bloomFilter.filter = filter
	return bloomFilter, nil
}

// Insert writes new _data_ in the bloom filter
func (bloomFilter *BloomFilter) Insert(data []byte) *BloomFilter {
	if isBitSetMem(bloomFilter.filter) {
		bloomFilter.lock.Lock()
		defer bloomFilter.lock.Unlock()
	}

	hashes := getHashes(data)
	if isBitSetMem(bloomFilter.filter) {
		for i := uint(0); i < bloomFilter.numHashes; i++ {
			bloomFilter.filter.insert(bloomFilter.getIndex(hashes, i))
		}
	} else {
		indexes := make([]uint, bloomFilter.numHashes)
		for i := uint(0); i < bloomFilter.numHashes; i++ {
			indexes[i] = bloomFilter.getIndex(hashes, i)
		}
		bloomFilter.filter.insertMulti(indexes)
	}
	return bloomFilter
}

// GetCap returns the size of the bloom filter
func (bloomFilter *BloomFilter) GetCap() uint {
	return bloomFilter.size
}

// GetNumHashes returns the number of hash functions used in the bloom filter
func (bloomFilter *BloomFilter) GetNumHashes() uint {
	return bloomFilter.numHashes
}

// GetBitSet returns the internal bitset. It would be a BitSetMem in case of an
// in-memory Bloom filter while it would be a BitSetRedis for a Redis backed
// Bloom filter.
func (bloomFilter *BloomFilter) GetBitSet() *IBitSet {
	return &bloomFilter.filter
}

// GetMetadataKey returns the Redis key used to store the metadata about the Redis
// backed Bloom filter
func (bloomFilter *BloomFilter) GetMetadataKey() string {
	return bloomFilter.metadataKey
}

// Lookup returns true if the corresponding bits in the bitset for _data_ is set,
// otherwise false
func (bloomFilter *BloomFilter) Lookup(data []byte) bool {
	if isBitSetMem(bloomFilter.filter) {
		bloomFilter.lock.Lock()
		defer bloomFilter.lock.Unlock()
	}

	hashes := getHashes(data)
	// if bitset.IsBitSetMem(bloomFilter.filter) {
	for i := uint(0); i < bloomFilter.numHashes; i++ {
		if ok, _ := bloomFilter.filter.has(bloomFilter.getIndex(hashes, i)); !ok {
			return false
		}
	}
	return true
	// } else {
	// 	indexes := make([]uint, bloomFilter.numHashes)
	// 	for i := uint(0); i < bloomFilter.numHashes; i++ {
	// 		indexes[i] = bloomFilter.getIndex(hashes, i)
	// 	}
	// 	result, _ := bloomFilter.filter.HasMulti(indexes)
	// 	for i := range result {
	// 		if !result[i] {
	// 			return false
	// 		}
	// 	}
	// 	return true
	// }
}

// InsertString accepts string value as _data_ for inserting into the Bloom filter
func (bloomFilter *BloomFilter) InsertString(data string) *BloomFilter {
	return bloomFilter.Insert([]byte(data))
}

// LookupString accepts string value as _data_ to lookup the Bloom filter
func (bloomFilter *BloomFilter) LookupString(data string) bool {
	return bloomFilter.Lookup([]byte(data))
}

// BloomPositiveRate returns the false positive error rate of the filter
func (bloomFilter *BloomFilter) BloomPositiveRate() float64 {
	length, _ := bloomFilter.filter.bitCount()
	return math.Pow(1-math.Exp(-float64(length)/float64(bloomFilter.size)), float64(bloomFilter.numHashes))
}

// Equals checks if two BloomFilter's are equal
func (aFilter *BloomFilter) Equals(bFilter *BloomFilter) (bool, error) {
	if aFilter.size != bFilter.size || aFilter.numHashes != bFilter.numHashes {
		return false, nil
	}
	ok, err := aFilter.filter.equals(bFilter.filter)
	if err != nil {
		return false, err
	}
	return ok, nil
}

// internal type used to marshal/unmarshal BloomFilter
type bloomFilterType struct {
	M uint   `json:"m"`
	K uint   `json:"k"`
	B []byte `json:"b"`
}

// Export JSON marshals the BloomFilter and returns a byte slice containing the data
func (bloomFilter *BloomFilter) Export() ([]byte, error) {
	_, bitset, err := bloomFilter.filter.marshal()
	if err != nil {
		return nil, err
	}
	return json.Marshal(bloomFilterType{bloomFilter.size, bloomFilter.numHashes, bitset})
}

// Import JSON unmarshals the _data_ into the BloomFilter
func (bloomFilter *BloomFilter) Import(data []byte) error {
	var f bloomFilterType
	err := json.Unmarshal(data, &f)
	if err != nil {
		return err
	}
	bloomFilter.size = f.M
	bloomFilter.numHashes = f.K
	_, err = bloomFilter.filter.unmarshal(f.B)
	return err
}

// WriteTo writes the BloomFilter onto the specified _stream_ and returns the
// number of bytes written.
// It can be used to write to disk (using a file stream) or to network.
// It's not implemented for Redis backed Bloom filter (BitSetRedis) as data for
// a Redis backed Bloom Filter is already there in Redis.
func (bloomFilter *BloomFilter) WriteTo(stream io.Writer) (int64, error) {
	if !isBitSetMem(bloomFilter.filter) {
		return 0, fmt.Errorf("stream write doesn't support bitset redis")
	}
	err := binary.Write(stream, binary.BigEndian, uint64(bloomFilter.size))
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, uint64(bloomFilter.numHashes))
	if err != nil {
		return 0, err
	}
	numBytes, err := bloomFilter.filter.writeTo(stream)
	return numBytes + int64(2*binary.Size(uint64(0))), err
}

// ReadFrom reads the BloomFilter from the specified _stream_ and returns the
// number of bytes read.
// It can be used to read from disk (using a file stream) or from network.
// It's not implemented for Redis backed Bloom filter (BitSetRedis) as data for
// a Redis backed Bloom Filter is already there in Redis. NewRedisBloomFilterFromKey
// method can be used to import or create a BloomFilter instead
func (bloomFilter *BloomFilter) ReadFrom(stream io.Reader) (int64, error) {
	var size, numHashes uint64
	err := binary.Read(stream, binary.BigEndian, &size)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &numHashes)
	if err != nil {
		return 0, err
	}
	bitSet := &BitSetMem{}
	numBytes, err := bitSet.readFrom(stream)
	if err != nil {
		return 0, err
	}
	bloomFilter.size = uint(size)
	bloomFilter.numHashes = uint(numHashes)
	bloomFilter.filter = bitSet
	return numBytes + int64(2*binary.Size(uint64(0))), nil
}

func getHashes(data []byte) [2]uint64 {
	hash1, hash2 := metro.Hash128(data, 1373)
	return [2]uint64{hash1, hash2}
}

func (bloomFilter *BloomFilter) getIndex(hashes [2]uint64, i uint) uint {
	j := uint64(i)
	return uint(math.Abs(float64((hashes[0] + j*hashes[1] + uint64(math.Floor(float64(math.Pow(float64(j), 3)-float64(j))/6))) % uint64(bloomFilter.size))))
}

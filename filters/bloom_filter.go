package filters

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
	"github.com/kwertop/gostatix"
	"github.com/kwertop/gostatix/bitset"
)

type BloomFilter struct {
	size        uint
	numHashes   uint
	filter      bitset.IBitSet
	metadataKey string
	lock        sync.RWMutex
}

func NewBloomFilterWithBitSet(size, numHashes uint, filter bitset.IBitSet, metadataKey string) (*BloomFilter, error) {
	if filter.Size() != size {
		return nil, fmt.Errorf("gostatix: error initializing filter as size of bitset %v doesn't match with size %v passed", filter.Size(), size)
	}
	return &BloomFilter{
		size:        gostatix.Max(size, 1),
		numHashes:   gostatix.Max(numHashes, 1),
		filter:      filter,
		metadataKey: metadataKey,
	}, nil
}

func NewRedisBloomFilterWithParameters(numItems uint, errorRate float64) (*BloomFilter, error) {
	size := gostatix.CalculateFilterSize(numItems, errorRate)
	numHashes := gostatix.CalculateNumHashes(size, numItems)
	filter := bitset.NewBitSetRedis(size)
	metadataKey := gostatix.GenerateRandomString(16)
	metadata := make(map[string]interface{})
	metadata["size"] = size
	metadata["numHashes"] = numHashes
	metadata["bitsetKey"] = filter.Key()
	err := gostatix.GetRedisClient().HSet(context.Background(), metadataKey, metadata).Err()
	if err != nil {
		return nil, fmt.Errorf("gostatix: error while creating bloom filter redis. error: %v", err)
	}
	return NewBloomFilterWithBitSet(size, numHashes, filter, metadataKey)
}

func NewMemBloomFilterWithParameters(numItems uint, errorRate float64) (*BloomFilter, error) {
	size := gostatix.CalculateFilterSize(numItems, errorRate)
	numHashes := gostatix.CalculateNumHashes(size, numItems)
	filter := bitset.NewBitSetMem(size)
	return NewBloomFilterWithBitSet(gostatix.Max(size, 1), gostatix.Max(numHashes, 1), filter, "")
}

func NewRedisBloomFilterFromBitSet(data []uint64, numHashes uint) (*BloomFilter, error) {
	size := gostatix.Max(uint(len(data)*64), 1)
	numHashes = gostatix.Max(numHashes, 1)
	metadataKey := gostatix.GenerateRandomString(16)
	err := gostatix.GetRedisClient().HSet(context.Background(), metadataKey, map[string]interface{}{"size": size, "numHashes": numHashes}).Err()
	if err != nil {
		return nil, fmt.Errorf("gostatix: error while creating bloom filter redis. error: %v", err)
	}
	bitSetRedis, err := bitset.FromDataRedis(data)
	if err != nil {
		return nil, err
	}
	return &BloomFilter{
		size:      size,
		numHashes: numHashes,
		filter:    bitSetRedis,
	}, nil
}

func NewMemBloomFilterFromBitSet(data []uint64, numHashes uint) *BloomFilter {
	size := uint(len(data) * 64)
	return &BloomFilter{size: gostatix.Max(size, 1), numHashes: gostatix.Max(numHashes, 1), filter: bitset.FromDataMem(data)}
}

func NewRedisBloomFilterFromKey(metadataKey string) (*BloomFilter, error) {
	values, err := gostatix.GetRedisClient().HGetAll(context.Background(), metadataKey).Result()
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
	filter, _ := bitset.FromRedisKey(bitsetKey)
	bloomFilter.filter = filter
	return bloomFilter, nil
}

func (bloomFilter *BloomFilter) Insert(data []byte) *BloomFilter {
	bloomFilter.lock.Lock()
	defer bloomFilter.lock.Unlock()

	hashes := getHashes(data)
	if bitset.IsBitSetMem(bloomFilter.filter) {
		for i := uint(0); i < bloomFilter.numHashes; i++ {
			bloomFilter.filter.Insert(bloomFilter.getIndex(hashes, i))
		}
	} else {
		indexes := make([]uint, bloomFilter.numHashes)
		for i := uint(0); i < bloomFilter.numHashes; i++ {
			indexes[i] = bloomFilter.getIndex(hashes, i)
		}
		bloomFilter.filter.InsertMulti(indexes)
	}
	return bloomFilter
}

func getHashes(data []byte) [2]uint64 {
	hash1, hash2 := metro.Hash128(data, 1373)
	return [2]uint64{hash1, hash2}
}

func (bloomFilter *BloomFilter) getIndex(hashes [2]uint64, i uint) uint {
	j := uint64(i)
	return uint(math.Abs(float64((hashes[0] + j*hashes[1] + uint64(math.Floor(float64(math.Pow(float64(j), 3)-float64(j))/6))) % uint64(bloomFilter.size))))
}

func (bloomFilter *BloomFilter) GetCap() uint {
	return bloomFilter.size
}

func (bloomFilter *BloomFilter) GetNumHashes() uint {
	return bloomFilter.numHashes
}

func (bloomFilter *BloomFilter) GetBitSet() *bitset.IBitSet {
	return &bloomFilter.filter
}

func (bloomFilter *BloomFilter) GetMetadataKey() string {
	return bloomFilter.metadataKey
}

func (bloomFilter *BloomFilter) Lookup(data []byte) bool {
	bloomFilter.lock.Lock()
	defer bloomFilter.lock.Unlock()

	hashes := getHashes(data)
	// if bitset.IsBitSetMem(bloomFilter.filter) {
	for i := uint(0); i < bloomFilter.numHashes; i++ {
		if ok, _ := bloomFilter.filter.Has(bloomFilter.getIndex(hashes, i)); !ok {
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

func (bloomFilter *BloomFilter) InsertString(data string) *BloomFilter {
	return bloomFilter.Insert([]byte(data))
}

func (bloomFilter *BloomFilter) LookupString(data string) bool {
	return bloomFilter.Lookup([]byte(data))
}

func (bloomFilter *BloomFilter) BloomPositiveRate() float64 {
	length, _ := bloomFilter.filter.BitCount()
	return math.Pow(1-math.Exp(-float64(length)/float64(bloomFilter.size)), float64(bloomFilter.numHashes))
}

func (aFilter *BloomFilter) Equals(bFilter *BloomFilter) (bool, error) {
	if aFilter.size != bFilter.size || aFilter.numHashes != bFilter.numHashes {
		return false, nil
	}
	ok, err := aFilter.filter.Equals(bFilter.filter)
	if err != nil {
		return false, err
	}
	return ok, nil
}

type bloomFilterType struct {
	M uint   `json:"m"`
	K uint   `json:"k"`
	B []byte `json:"b"`
}

func (bloomFilter *BloomFilter) Export() ([]byte, error) {
	_, bitset, err := bloomFilter.filter.Export()
	if err != nil {
		return nil, err
	}
	return json.Marshal(bloomFilterType{bloomFilter.size, bloomFilter.numHashes, bitset})
}

func (bloomFilter *BloomFilter) Import(data []byte) error {
	var f bloomFilterType
	err := json.Unmarshal(data, &f)
	if err != nil {
		return err
	}
	bloomFilter.size = f.M
	bloomFilter.numHashes = f.K
	_, err = bloomFilter.filter.Import(f.B)
	return err
}

func (bloomFilter *BloomFilter) WriteTo(stream io.Writer) (int64, error) {
	if !bitset.IsBitSetMem(bloomFilter.filter) {
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
	numBytes, err := bloomFilter.filter.WriteTo(stream)
	return numBytes + int64(2*binary.Size(uint64(0))), err
}

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
	bitSet := &bitset.BitSetMem{}
	numBytes, err := bitSet.ReadFrom(stream)
	if err != nil {
		return 0, err
	}
	bloomFilter.size = uint(size)
	bloomFilter.numHashes = uint(numHashes)
	bloomFilter.filter = bitSet
	return numBytes + int64(2*binary.Size(uint64(0))), nil
}

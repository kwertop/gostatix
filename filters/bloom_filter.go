package filters

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/kwertop/gostatix"
	"github.com/kwertop/gostatix/bitset"
	"github.com/kwertop/gostatix/hash"
)

type BloomFilter struct {
	size      uint
	numHashes uint
	filter    bitset.IBitSet
}

func NewBloomFilterWithBitSet(size, numHashes uint, filter bitset.IBitSet) (*BloomFilter, error) {
	if filter.Size() != size {
		return nil, fmt.Errorf("gostatix: error initializing filter as size of bitset %v doesn't match with size %v passed", filter.Size(), size)
	}
	return &BloomFilter{gostatix.Max(size, 1), gostatix.Max(numHashes, 1), filter}, nil
}

func NewRedisBloomFilterWithParameters(numItems uint, errorRate float64, redisUrl, key string) (*BloomFilter, error) {
	size := gostatix.CalculateFilterSize(numItems, errorRate)
	numHashes := gostatix.CalculateNumHashes(size, numItems)
	filter := bitset.NewBitSetRedis(size, key)
	return NewBloomFilterWithBitSet(gostatix.Max(size, 1), gostatix.Max(numHashes, 1), filter)
}

func NewMemBloomFilterWithParameters(numItems uint, errorRate float64) (*BloomFilter, error) {
	size := gostatix.CalculateFilterSize(numItems, errorRate)
	numHashes := gostatix.CalculateNumHashes(size, numItems)
	filter := bitset.NewBitSetMem(size)
	return NewBloomFilterWithBitSet(gostatix.Max(size, 1), gostatix.Max(numHashes, 1), filter)
}

func NewRedisBloomFilterFromBitSet(data []uint64, numHashes uint, key string) (*BloomFilter, error) {
	size := uint(len(data) * 64)
	bitSetRedis, err := bitset.FromDataRedis(data, key)
	if err != nil {
		return nil, err
	}
	return &BloomFilter{gostatix.Max(size, 1), gostatix.Max(numHashes, 1), bitSetRedis}, nil
}

func NewMemBloomFilterFromBitSet(data []uint64, numHashes uint) *BloomFilter {
	size := uint(len(data) * 64)
	return &BloomFilter{gostatix.Max(size, 1), gostatix.Max(numHashes, 1), bitset.FromDataMem(data)}
}

func (bloomFilter *BloomFilter) Insert(data []byte) *BloomFilter {
	hashes := getHashes(data)
	for i := uint(0); i < bloomFilter.numHashes; i++ {
		bloomFilter.filter.Insert(bloomFilter.getIndex(hashes, i))
	}
	return bloomFilter
}

func getHashes(data []byte) [2]uint64 {
	hash1, hash2 := hash.Sum128(data)
	return [2]uint64{hash1, hash2}
}

func (bloomFilter BloomFilter) getIndex(hashes [2]uint64, i uint) uint {
	j := uint64(i)
	return uint(math.Abs(float64((hashes[0] + j*hashes[1] + uint64(math.Floor(float64(math.Pow(float64(j), 3)-float64(j))/6))) % uint64(bloomFilter.size))))
}

func (bloomFilter BloomFilter) GetCap() uint {
	return bloomFilter.size
}

func (bloomFilter BloomFilter) GetNumHashes() uint {
	return bloomFilter.numHashes
}

func (bloomFilter BloomFilter) GetBitSet() *bitset.IBitSet {
	return &bloomFilter.filter
}

func (bloomFilter BloomFilter) Lookup(data []byte) bool {
	hashes := getHashes(data)
	for i := uint(0); i < bloomFilter.numHashes; i++ {
		if ok, _ := bloomFilter.filter.Has(bloomFilter.getIndex(hashes, i)); !ok {
			return false
		}
	}
	return true
}

func (bloomFilter BloomFilter) InsertString(data string) *BloomFilter {
	return bloomFilter.Insert([]byte(data))
}

func (bloomFilter BloomFilter) LookupString(data string) bool {
	return bloomFilter.Lookup([]byte(data))
}

func (bloomFilter BloomFilter) BloomPositiveRate() float64 {
	length, _ := bloomFilter.filter.BitCount()
	return math.Pow(1-math.Exp(-float64(length)/float64(bloomFilter.size)), float64(bloomFilter.numHashes))
}

func (aFilter BloomFilter) Equals(bFilter BloomFilter) (bool, error) {
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

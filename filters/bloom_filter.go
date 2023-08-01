package filters

import (
	"math"

	"github.com/gostatix"
	"github.com/gostatix/bitset"
	"github.com/gostatix/hash"
)

type BloomFilter struct {
	size      uint
	numHashes uint
	filter    bitset.IBitSet
}

func NewBloomFilterWithBitSet(size, numHashes uint, filter bitset.IBitSet) *BloomFilter {
	return &BloomFilter{size, numHashes, filter}
}

func NewRedisBloomFilterWithParameters(numItems uint, errorRate float64, redisUrl, key string) (*BloomFilter, error) {
	size := gostatix.CalculateFilterSize(numItems, errorRate)
	numHashes := gostatix.CalculateNumHashes(size, numItems)
	filter := bitset.NewBitSetRedis(size, key)
	return NewBloomFilterWithBitSet(size, numHashes, filter), nil
}

func NewMemBloomFilterWithParameters(numItems uint, errorRate float64) *BloomFilter {
	size := gostatix.CalculateFilterSize(numItems, errorRate)
	numHashes := gostatix.CalculateNumHashes(size, numItems)
	filter := bitset.NewBitSetMem(size)
	return NewBloomFilterWithBitSet(size, numHashes, filter)
}

func NewRedisBloomFilterFromBitSet(data []uint64, numHashes uint, key string) (*BloomFilter, error) {
	size := uint(len(data) * 64)
	bitSetRedis, err := bitset.FromDataRedis(data, key)
	if err != nil {
		return nil, err
	}
	return &BloomFilter{size, numHashes, bitSetRedis}, nil
}

func NewMemBloomFilterFromBitSet(data []uint64, numHashes uint) *BloomFilter {
	size := uint(len(data) * 64)
	return &BloomFilter{size, numHashes, bitset.FromDataMem(data)}
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
	ok, err := aFilter.filter.Equals(bFilter.filter)
	if err != nil {
		return false, err
	}
	return ok, nil
}

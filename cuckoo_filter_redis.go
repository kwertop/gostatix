/*
Provides data structures and methods for creating probabilistic filters.
This package provides implementation Cuckoo Filter.
*/
package gostatix

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strconv"

	"github.com/kwertop/gostatix/internal/util"
	"github.com/redis/go-redis/v9"
)

// CuckooFilterRedis is the Redis backed implementation of BaseCuckooFilter
// _buckets_ is a slice of BucketRedis
// _key_ holds the Redis key to the list which has the Redis keys of all buckets
// _metadataKey_ is used to store the additional information about CuckooFilterRedis
// for retrieving the filter by the Redis key
type CuckooFilterRedis struct {
	buckets     map[string]*BucketRedis
	key         string
	metadataKey string
	*AbstractCuckooFilter
}

// NewCuckooFilter creates a new CuckooFilterRedis
// _size_ is the size of the BucketRedis slice
// _bucketSize_ is the size of the individual buckets inside the bucket slice
// _fingerPrintLength_ is fingerprint hash of the input to be inserted/removed/lookup
func NewCuckooFilterRedis(size, bucketSize, fingerPrintLength uint64) (*CuckooFilterRedis, error) {
	return NewCuckooFilterRedisWithRetries(size, bucketSize, fingerPrintLength, 500)
}

// NewCuckooFilterWithRetries creates new CuckooFilterRedis with specified _retries_
// _size_ is the size of the BucketRedis slice
// _bucketSize_ is the size of the individual buckets inside the bucket slice
// _fingerPrintLength_ is fingerprint hash of the input to be inserted/removed/lookup
// _retries_ is the number of retries that the Cuckoo filter makes if the first two indices obtained
// after hashing the input is already occupied in the filter
func NewCuckooFilterRedisWithRetries(size, bucketSize, fingerPrintLength, retries uint64) (*CuckooFilterRedis, error) {
	filterKey := util.GenerateRandomString(16)
	baseFilter := makeAbstractCuckooFilter(size, bucketSize, fingerPrintLength, retries)
	metadataKey := util.GenerateRandomString(16)
	filter := &CuckooFilterRedis{make(map[string]*BucketRedis, size), filterKey, metadataKey, baseFilter}
	err := filter.setMetadata(0)
	if err != nil {
		return nil, fmt.Errorf("gostatix: error while creating cuckoo filter redis. error: %v", err)
	}
	err = filter.initBuckets()
	if err != nil {
		return nil, fmt.Errorf("gostatix: error while creating cuckoo filter redis: %v", err)
	}
	return filter, nil
}

// NewCuckooFilterWithErrorRate creates an CuckooFilterRedis with a specified false positive
// rate : _errorRate_
// _size_ is the size of the BucketRedis slice
// _bucketSize_ is the size of the individual buckets inside the bucket slice
// _retries_ is the number of retries that the Cuckoo filter makes if the first two indices obtained
// _errorRate_ is the desired false positive rate of the filter. fingerPrintLength is calculated
// according to this error rate.
func NewCuckooFilterRedisWithErrorRate(size, bucketSize, retries uint64, errorRate float64) (*CuckooFilterRedis, error) {
	fingerPrintLength := util.CalculateFingerPrintLength(size, errorRate)
	capacity := uint64(math.Ceil(float64(size) * 0.955 / float64(bucketSize)))
	return NewCuckooFilterRedisWithRetries(capacity, bucketSize, fingerPrintLength, retries)
}

// NewCuckooFilterRedisFromKey is used to create a new Redis backed Cuckoo Filter from the
// _metadataKey_ (the Redis key used to store the metadata about the cuckoo filter) passed
// For this to work, value should be present in Redis at _key_
func NewCuckooFilterRedisFromKey(metadataKey string) (*CuckooFilterRedis, error) {
	values, err := getRedisClient().HGetAll(context.Background(), metadataKey).Result()
	if err != nil {
		return nil, fmt.Errorf("gostatix: error while fetching hash from redis, error: %v", err)
	}
	size, _ := strconv.Atoi(values["size"])
	bucketSize, _ := strconv.Atoi(values["bucketSize"])
	fingerPrintLength, _ := strconv.Atoi(values["fingerPrintLength"])
	retries, _ := strconv.Atoi(values["retries"])
	cuckooFilter := &CuckooFilterRedis{}
	baseFilter := makeAbstractCuckooFilter(uint64(size), uint64(bucketSize), uint64(fingerPrintLength), uint64(retries))
	cuckooFilter.AbstractCuckooFilter = baseFilter
	cuckooFilter.metadataKey = metadataKey
	cuckooFilter.key = values["key"]
	cuckooFilter.buckets = make(map[string]*BucketRedis)
	cuckooFilter.localInitBuckets()
	return cuckooFilter, nil
}

// Key returns the value of the _key_ to the Redis list where all bucket keys are stored
func (cuckooFilter CuckooFilterRedis) Key() string {
	return cuckooFilter.key
}

// MetadataKey return _metadataKey_
func (cuckooFilter CuckooFilterRedis) MetadataKey() string {
	return cuckooFilter.metadataKey
}

// Length returns the current length of the Cuckoo Filter or the current number of entries
// present in the Cuckoo Filter. In CuckooFilterRedis, length is tracked using a key-value pair
// in Redis and modified using INCRBY Redis command
func (cuckooFilter *CuckooFilterRedis) Length() uint64 {
	value, _ := getRedisClient().HGet(context.Background(), cuckooFilter.metadataKey, "length").Int64()
	return uint64(value)
}

// Insert writes the _data_ in the Cuckoo Filter for future lookup
// _destructive_ parameter is used to specify if the previous ordering of the
// present entries is to be preserved after the retries (if that case arises)
func (cuckooFilter *CuckooFilterRedis) Insert(data []byte, destructive bool) bool {
	fingerPrint, firstBucketIndex, secondBucketIndex, _ := cuckooFilter.getPositions(data)
	fIndex := cuckooFilter.getIndexKey(firstBucketIndex)
	sIndex := cuckooFilter.getIndexKey(secondBucketIndex)
	if cuckooFilter.buckets[fIndex].isFree() {
		cuckooFilter.buckets[fIndex].add(fingerPrint)
	} else if cuckooFilter.buckets[sIndex].isFree() {
		cuckooFilter.buckets[sIndex].add(fingerPrint)
	} else {
		var index uint64
		if rand.Float32() < 0.5 {
			index = firstBucketIndex
		} else {
			index = secondBucketIndex
		}
		indexKey := "cuckoo_" + cuckooFilter.key + "_bucket_" + strconv.FormatUint(index, 10)
		currFingerPrint := fingerPrint
		var items []entry
		for i := uint64(0); i < cuckooFilter.retries; i++ {
			randIndex := uint64(math.Ceil(rand.Float64() * float64(cuckooFilter.buckets[indexKey].getLength()-1)))
			prevFingerPrint, _ := cuckooFilter.buckets[indexKey].at(randIndex)
			items = append(items, entry{prevFingerPrint, index, randIndex})
			cuckooFilter.buckets[indexKey].set(randIndex, currFingerPrint)
			hash := getHash([]byte(prevFingerPrint))
			newIndex := (index ^ hash) % uint64(len(cuckooFilter.buckets))
			newIndexKey := "cuckoo_" + cuckooFilter.key + "_bucket_" + strconv.FormatUint(newIndex, 10)
			if cuckooFilter.buckets[newIndexKey].isFree() {
				cuckooFilter.buckets[newIndexKey].add(prevFingerPrint)
				cuckooFilter.incrLength()
				return true
			}
		}
		if !destructive {
			for i := len(items) - 1; i >= 0; i-- {
				item := items[i]
				firstIndexKey := "cuckoo_" + cuckooFilter.key + "_bucket_" + strconv.FormatUint(item.firstIndex, 10)
				cuckooFilter.buckets[firstIndexKey].set(item.secondIndex, item.fingerPrint)
			}
		}
		panic("cannot insert element, cuckoofilter is full")
	}
	cuckooFilter.incrLength()
	return true
}

// Lookup returns true if the _data_ is present in the Cuckoo Filter, else false
func (cuckooFilter *CuckooFilterRedis) Lookup(data []byte) (bool, error) {
	fingerPrint, firstBucketIndex, secondBucketIndex, _ := cuckooFilter.getPositions(data)
	fIndex := cuckooFilter.getIndexKey(firstBucketIndex)
	sIndex := cuckooFilter.getIndexKey(secondBucketIndex)
	isAtFirstIndex, err := cuckooFilter.buckets[fIndex].lookup(fingerPrint)
	if err != nil {
		return false, fmt.Errorf("gostatix: error while lookup of data: %v", err)
	}
	if isAtFirstIndex {
		return isAtFirstIndex, nil
	}
	isAtSecondIndex, err := cuckooFilter.buckets[sIndex].lookup(fingerPrint)
	if err != nil {
		return false, fmt.Errorf("gostatix: error while lookup of data: %v", err)
	}
	return isAtFirstIndex || isAtSecondIndex, nil
}

// Remove deletes the _data_ from the Cuckoo Filter
func (cuckooFilter *CuckooFilterRedis) Remove(data []byte) (bool, error) {
	fingerPrint, firstBucketIndex, secondBucketIndex, _ := cuckooFilter.getPositions(data)
	fIndex := cuckooFilter.getIndexKey(firstBucketIndex)
	sIndex := cuckooFilter.getIndexKey(secondBucketIndex)
	isPresent, err := cuckooFilter.buckets[fIndex].lookup(fingerPrint)
	if err != nil {
		return false, fmt.Errorf("gostatix: error while removing the data, error: %v", err)
	}
	if isPresent {
		cuckooFilter.buckets[fIndex].remove(fingerPrint)
		cuckooFilter.decrLength()
		return true, nil
	}
	isPresent, err = cuckooFilter.buckets[sIndex].lookup(fingerPrint)
	if err != nil {
		return false, fmt.Errorf("gostatix: error while removing the data, error: %v", err)
	}
	if isPresent {
		cuckooFilter.buckets[sIndex].remove(fingerPrint)
		cuckooFilter.decrLength()
		return true, nil
	}
	return false, nil
}

// bucketRedisJSON is internal struct used to json marshal/unmarshal redis backed buckets
type bucketRedisJSON struct {
	Size     uint64   `json:"s"`
	Length   uint64   `json:"l"`
	Elements []string `json:"e"`
	Key      string   `json:"k"`
}

// cuckooFilterRedisJSON is internal struct used to json marshal/unmarshal redis backed cuckoo filter
type cuckooFilterRedisJSON struct {
	Size              uint64            `json:"s"`
	BucketSize        uint64            `json:"bs"`
	FingerPrintLength uint64            `json:"fpl"`
	Length            uint64            `json:"l"`
	Retries           uint64            `json:"r"`
	Buckets           []bucketRedisJSON `json:"b"`
	Key               string            `json:"k"`
	MetadataKey       string            `json:"mk"`
}

// Export JSON marshals the CuckooFilterRedis and returns a byte slice containing the data
func (filter *CuckooFilterRedis) Export() ([]byte, error) {
	bucketsJSON := make([]bucketRedisJSON, filter.size)
	for i := uint64(0); i < filter.size; i++ {
		bucketKey := filter.getIndexKey(i)
		bucket := filter.buckets[bucketKey]
		elements, _ := bucket.getElements()
		bucketJSON := bucketRedisJSON{bucket.Size(), bucket.getLength(), elements, bucketKey}
		bucketsJSON[i] = bucketJSON
	}
	return json.Marshal(cuckooFilterRedisJSON{
		filter.size,
		filter.bucketSize,
		filter.fingerPrintLength,
		filter.Length(),
		filter.retries,
		bucketsJSON,
		filter.key,
		filter.metadataKey,
	})
}

// Import JSON unmarshals the _data_ into the CuckooFilterRedis
func (filter *CuckooFilterRedis) Import(data []byte, withNewRedisKey bool) error {
	var f cuckooFilterRedisJSON
	err := json.Unmarshal(data, &f)
	if err != nil {
		return fmt.Errorf("gostatix: error importing data, error %v", err)
	}
	filter.size = f.Size
	filter.bucketSize = f.BucketSize
	filter.fingerPrintLength = f.FingerPrintLength
	filter.retries = f.Retries
	if withNewRedisKey {
		filter.key = util.GenerateRandomString(16)
		filter.metadataKey = util.GenerateRandomString(16)
	} else {
		filter.key = f.Key
		filter.metadataKey = f.MetadataKey
	}
	filter.setMetadata(f.Length)
	filter.initBuckets()
	filters := make(map[string]*BucketRedis, f.Size)
	for i := range f.Buckets {
		bucketJSON := f.Buckets[i]
		bucketKey := filter.getIndexKey(uint64(i))
		bucket := newBucketRedis(bucketKey, f.BucketSize)
		for j := range bucketJSON.Elements {
			bucket.add(bucketJSON.Elements[j])
		}
		filters[bucketKey] = bucket
	}
	filter.buckets = filters
	return nil
}

func (aFilter CuckooFilterRedis) Equals(bFilter CuckooFilterRedis) (bool, error) {
	count := 0
	result := true
	for result && count < len(aFilter.buckets) {
		bucket := aFilter.buckets[aFilter.getIndexKey(uint64(count))]
		ok, err := bFilter.buckets[bFilter.getIndexKey(uint64(count))].equals(bucket)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		count++
	}
	return true, nil
}

func (cuckooFilter *CuckooFilterRedis) incrLength() error {
	return getRedisClient().HIncrBy(context.Background(), cuckooFilter.metadataKey, "length", 1).Err()
}

func (cuckooFilter *CuckooFilterRedis) decrLength() error {
	return getRedisClient().HIncrBy(context.Background(), cuckooFilter.metadataKey, "length", -1).Err()
}

func (cuckooFilter *CuckooFilterRedis) setMetadata(length uint64) error {
	metadata := make(map[string]interface{})
	metadata["size"] = cuckooFilter.size
	metadata["bucketSize"] = cuckooFilter.bucketSize
	metadata["fingerPrintLength"] = cuckooFilter.fingerPrintLength
	metadata["retries"] = cuckooFilter.retries
	metadata["key"] = cuckooFilter.key
	metadata["length"] = 0
	return getRedisClient().HSet(context.Background(), cuckooFilter.metadataKey, metadata).Err()
}

func (filter *CuckooFilterRedis) initBuckets() error {
	var bucketKeys []string
	for i := uint64(0); i < filter.size; i++ {
		bucketKey := "cuckoo_" + filter.key + "_bucket_" + strconv.FormatUint(i, 10)
		bucketKeys = append(bucketKeys, bucketKey)
	}
	initCuckooFilterRedis := redis.NewScript(`
		local key = KEYS[1]
		local size = ARGV[1]
		local bucketSize = ARGV[2]
		redis.call("DEL", key)
		for i=2, tonumber(size)+1 do
			redis.call("LPUSH", key, KEYS[i])
		end
		return true
	`)
	_, err := initCuckooFilterRedis.Run(
		context.Background(),
		getRedisClient(),
		append([]string{filter.key}, bucketKeys...),
		filter.size,
		filter.bucketSize,
	).Bool()
	if err != nil {
		return fmt.Errorf("error while init buckets in redis, error: %v", err)
	}
	for i := range bucketKeys {
		bucketKey := bucketKeys[i]
		filter.buckets[bucketKey] = newBucketRedis(bucketKey, filter.bucketSize)
	}
	return nil
}

func (filter *CuckooFilterRedis) localInitBuckets() {
	var bucketKeys []string
	for i := uint64(0); i < filter.size; i++ {
		bucketKey := "cuckoo_" + filter.key + "_bucket_" + strconv.FormatUint(i, 10)
		bucketKeys = append(bucketKeys, bucketKey)
	}
	for i := range bucketKeys {
		bucketKey := bucketKeys[i]
		filter.buckets[bucketKey] = newBucketRedis(bucketKey, filter.bucketSize)
	}
}

func (cuckooFilter *CuckooFilterRedis) getIndexKey(index uint64) string {
	return "cuckoo_" + cuckooFilter.key + "_bucket_" + strconv.FormatUint(index, 10)
}

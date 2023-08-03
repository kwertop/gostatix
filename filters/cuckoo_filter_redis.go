package filters

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"

	"github.com/gostatix"
	"github.com/gostatix/buckets"
	"github.com/redis/go-redis/v9"
)

type CuckooFilterRedis struct {
	buckets map[string]*buckets.BucketRedis
	key     string
	*AbstractCuckooFilter
}

func NewCuckooFilterRedis(size, bucketSize, fingerPrintLength uint64) (*CuckooFilterRedis, error) {
	return NewCuckooFilterRedisWithRetries(size, bucketSize, fingerPrintLength, 500)
}

func NewCuckooFilterRedisWithRetries(size, bucketSize, fingerPrintLength, retries uint64) (*CuckooFilterRedis, error) {
	filterKey := gostatix.GenerateRandomString(16)
	baseFilter := MakeAbstractCuckooFilter(size, bucketSize, fingerPrintLength, 0, retries)
	filter := &CuckooFilterRedis{make(map[string]*buckets.BucketRedis, size), filterKey, baseFilter}
	err := filter.initBuckets()
	if err != nil {
		return nil, fmt.Errorf("gostatix: error while creating new cuckoo filter redis: %v", err)
	}
	return filter, nil
}

func NewCuckooFilterRedisWithErrorRate(size, bucketSize, retries uint64, errorRate float64) (*CuckooFilterRedis, error) {
	fingerPrintLength := gostatix.CalculateFingerPrintLength(size, errorRate)
	capacity := uint64(math.Ceil(float64(size) * 0.955 / float64(bucketSize)))
	return NewCuckooFilterRedisWithRetries(capacity, bucketSize, fingerPrintLength, retries)
}

func (cuckooFilter *CuckooFilterRedis) Insert(data []byte, destructive bool) bool {
	fingerPrint, firstBucketIndex, secondBucketIndex, _ := cuckooFilter.getPositions(data)
	fIndex := cuckooFilter.getIndexKey(firstBucketIndex)
	sIndex := cuckooFilter.getIndexKey(secondBucketIndex)
	if cuckooFilter.buckets[fIndex].IsFree() {
		cuckooFilter.buckets[fIndex].Add(fingerPrint)
	} else if cuckooFilter.buckets[sIndex].IsFree() {
		cuckooFilter.buckets[sIndex].Add(fingerPrint)
	} else {
		var index uint64
		if rand.Float32() < 0.5 {
			index = firstBucketIndex
		} else {
			index = secondBucketIndex
		}
		indexKey := "cuckoo_" + cuckooFilter.key + "_bucket_" + strconv.FormatUint(index, 10)
		currFingerPrint := fingerPrint
		var items []Entry
		for i := uint64(0); i < cuckooFilter.retries; i++ {
			randIndex := uint64(math.Ceil(rand.Float64() * float64(cuckooFilter.buckets[indexKey].Length()-1)))
			prevFingerPrint, _ := cuckooFilter.buckets[indexKey].At(index)
			items = append(items, Entry{prevFingerPrint, index, randIndex})
			cuckooFilter.buckets[indexKey].Set(randIndex, currFingerPrint)
			hash := getHash([]byte(prevFingerPrint))
			newIndex := (index ^ hash) % uint64(len(cuckooFilter.buckets))
			newIndexKey := "cuckoo_" + cuckooFilter.key + "_bucket_" + strconv.FormatUint(newIndex, 10)
			if cuckooFilter.buckets[newIndexKey].IsFree() {
				cuckooFilter.buckets[newIndexKey].Add(prevFingerPrint)
				cuckooFilter.length++
				return true
			}
		}
		if !destructive {
			for i := len(items); i >= 0; i-- {
				item := items[i]
				firstIndexKey := "cuckoo_" + cuckooFilter.key + "_bucket_" + strconv.FormatUint(item.firstIndex, 10)
				cuckooFilter.buckets[firstIndexKey].Set(item.secondIndex, item.fingerPrint)
			}
		}
		panic("cannot insert element, cuckoofilter is full")
	}
	cuckooFilter.length++
	return true
}

func (cuckooFilter *CuckooFilterRedis) Lookup(data []byte) (bool, error) {
	fingerPrint, firstBucketIndex, secondBucketIndex, _ := cuckooFilter.getPositions(data)
	fIndex := cuckooFilter.getIndexKey(firstBucketIndex)
	sIndex := cuckooFilter.getIndexKey(secondBucketIndex)
	isAtFirstIndex, err := cuckooFilter.buckets[fIndex].Lookup(fingerPrint)
	if err != nil {
		return false, fmt.Errorf("gostatix: error while lookup of data: %v", err)
	}
	isAtSecondIndex, err := cuckooFilter.buckets[sIndex].Lookup(fingerPrint)
	if err != nil {
		return false, fmt.Errorf("gostatix: error while lookup of data: %v", err)
	}
	return isAtFirstIndex || isAtSecondIndex, nil
}

func (cuckooFilter *CuckooFilterRedis) Remove(data []byte) (bool, error) {
	fingerPrint, firstBucketIndex, secondBucketIndex, _ := cuckooFilter.getPositions(data)
	fIndex := cuckooFilter.getIndexKey(firstBucketIndex)
	sIndex := cuckooFilter.getIndexKey(secondBucketIndex)
	isPresent, err := cuckooFilter.buckets[fIndex].Lookup(fingerPrint)
	if err != nil {
		return false, fmt.Errorf("gostatix: error while removing the data, error: %v", err)
	}
	if isPresent {
		cuckooFilter.buckets[fIndex].Remove(fingerPrint)
		cuckooFilter.length--
		return true, nil
	}
	isPresent, err = cuckooFilter.buckets[sIndex].Lookup(fingerPrint)
	if err != nil {
		return false, fmt.Errorf("gostatix: error while removing the data, error: %v", err)
	}
	if isPresent {
		cuckooFilter.buckets[sIndex].Remove(fingerPrint)
		cuckooFilter.length--
		return true, nil
	}
	return false, nil
}

func (aFilter CuckooFilterRedis) Equals(bFilter CuckooFilterRedis) (bool, error) {
	count := 0
	result := true
	for result && count < len(aFilter.buckets) {
		bucket := aFilter.buckets[aFilter.getIndexKey(uint64(count))]
		ok, err := bFilter.buckets[bFilter.getIndexKey(uint64(count))].Equals(bucket)
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
		local bucketKeys = ARGV[3]
		redis.call("DEL", key)
		for i=1, size do
			redis.call("LPUSH", key, bucketKeys[i-1])
		end
		for i=1, size do
			local bucketKey = bucketKeys[i-1]
			for j=1, bucketSize do
				redis.call("LPUSH", bucketKey, "")
			end
		end
	`)
	_, err := initCuckooFilterRedis.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		[]string{filter.key},
		filter.size,
		filter.bucketSize,
		bucketKeys,
	).Result()
	if err != nil {
		return fmt.Errorf("error while init buckets in redis, error: %v", err)
	}
	for i := range bucketKeys {
		bucketKey := bucketKeys[i]
		filter.buckets[bucketKey] = buckets.NewBucketRedis(bucketKey, filter.bucketSize)
	}
	return nil
}

func (cuckooFilter *CuckooFilterRedis) getIndexKey(index uint64) string {
	return "cuckoo_" + cuckooFilter.key + "_bucket_" + strconv.FormatUint(index, 10)
}
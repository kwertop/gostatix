package filters

import (
	"context"
	"fmt"
	"math"
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

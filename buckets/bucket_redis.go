package buckets

import (
	"context"
	"fmt"

	"github.com/kwertop/gostatix"
	"github.com/redis/go-redis/v9"
)

type BucketRedis struct {
	key string
	*AbstractBucket
}

func NewBucketRedis(key string, size uint64) *BucketRedis {
	bucket := &AbstractBucket{}
	bucket.size = size
	bucket.length = 0
	return &BucketRedis{key, bucket}
}

func (bucket BucketRedis) NextSlot() (int64, error) {
	lPosArgs := redis.LPosArgs{Rank: 1, MaxLen: 0}
	pos, err := gostatix.GetRedisClient().LPos(context.Background(), bucket.key, "", lPosArgs).Result()
	if err != nil {
		return -1, fmt.Errorf("gostatix: error while fetching next empty slot: %v", err)
	}
	return pos, nil
}

func (bucket BucketRedis) At(index uint64) (string, error) {
	val, err := gostatix.GetRedisClient().LIndex(context.Background(), bucket.key, int64(index)).Result()
	if err != nil {
		return "", fmt.Errorf("gostatix: error while fetching value at index: %v", err)
	}
	return val, nil
}

func (bucket *BucketRedis) Add(element string) (bool, error) {
	if element == "" || !bucket.IsFree() {
		return false, nil
	}
	addElement := redis.NewScript(`
		local key = KEYS[1]
		local element = ARGV[1]
		local pos = redis.call('LPOS', key, '')
		local reply = redis.call('LSET', key, tonumber(pos), element)
		return true
	`)
	_, err := addElement.Run(context.Background(), gostatix.GetRedisClient(), []string{bucket.key}, element).Bool()
	if err != nil {
		return false, fmt.Errorf("gostatix: error while adding element %s, error: %v", element, err)
	}
	bucket.length++
	return true, nil
}

func (bucket *BucketRedis) Remove(element string) (bool, error) {
	removeElement := redis.NewScript(`
		local key = KEYS[1]
		local element = ARGV[1]
		local pos = redis.call('LPOS', key, element)
		redis.call('LSET', key, pos, '')
		return true
	`)
	_, err := removeElement.Run(context.Background(), gostatix.GetRedisClient(), []string{bucket.key}, element).Bool()
	if err != nil {
		return false, fmt.Errorf("gostatix: error while removing element %s, error: %v", element, err)
	}
	bucket.length--
	return true, nil
}

func (bucket BucketRedis) Lookup(element string) (bool, error) {
	lPosArgs := redis.LPosArgs{Rank: 1, MaxLen: 0}
	pos, err := gostatix.GetRedisClient().LPos(context.Background(), bucket.key, element, lPosArgs).Result()
	if err != nil {
		return false, fmt.Errorf("gostatix: error while searching for %s, error: %v", element, err)
	}
	return pos > -1, nil
}

func (bucket BucketRedis) Set(index uint64, element string) error {
	_, err := gostatix.GetRedisClient().LSet(context.Background(), bucket.key, int64(index), element).Result()
	if err != nil {
		return fmt.Errorf("gostatix: error while setting element %s at index %d, error: %v", element, index, err)
	} else {
		return nil
	}
}

func (bucket *BucketRedis) UnSet(index uint64) error {
	_, err := gostatix.GetRedisClient().LSet(context.Background(), bucket.key, int64(index), "").Result()
	if err != nil {
		return fmt.Errorf("gostatix: error while unsetting index %d, error: %v", index, err)
	} else {
		bucket.length--
		return nil
	}
}

func (bucket *BucketRedis) Equals(otherBucket *BucketRedis) (bool, error) {
	if bucket.size != otherBucket.size || bucket.length != otherBucket.length {
		return false, nil
	}
	equals := redis.NewScript(`
		local key1 = KEYS[1]
		local key2 = KEYS[2]
		local size = ARGV[1]
		for i=1, tonumber(size) do
			local val1 = redis.pcall("LINDEX", key1, i)
			local val2 = redis.pcall("LINDEX", key2, i)
			if val1 ~= val2 then
				return false
			end
		end
		return true
	`)
	ok, err := equals.Run(context.Background(), gostatix.GetRedisClient(), []string{bucket.key, otherBucket.key}, bucket.size).Bool()
	if err != nil {
		return false, fmt.Errorf("gostatix: error while comparing list %s with %s, error: %v", bucket.key, otherBucket.key, err)
	}
	return ok, nil
}

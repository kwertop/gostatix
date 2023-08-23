/*
Package buckets implements buckets - a container of fixed number of entries
used in cuckoo filters.
*/
package gostatix

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// BucketRedis is data structure holding the key to the entries of the bucket
// saved in redis used for cuckoo filters.
// BucketRedis is implemented using Redis Lists.
// _key_ is the redis key to the list which holds the actual values
// _key_len is used to track the number of non-empty/valied entries in the bucket
// as a key-value pair in Redis.
// Lua scripts are used wherever possible to make the read/write operations from Redis atomic.
type BucketRedis struct {
	key string
	*AbstractBucket
}

// NewBucketRedis creates a new BucketRedis
func NewBucketRedis(key string, size uint64) *BucketRedis {
	bucket := &AbstractBucket{}
	bucket.size = size
	bucketRedis := &BucketRedis{}
	bucketRedis.key = key
	bucketRedis.AbstractBucket = bucket
	bucketRedis.incrLength()
	return bucketRedis
}

// Length returns the number of entries in the bucket
func (bucket *BucketRedis) Length() uint64 {
	val, _ := getRedisClient().Get(context.Background(), bucket.key+"_len").Int64()
	return uint64(val)
}

// IsFree returns true if there is room for more entries in the bucket,
// otherwise false.
func (bucket *BucketRedis) IsFree() bool {
	isFreeScript := redis.NewScript(`
		local key = KEYS[1]
		local lenKey = key .. '_len'
		local bucketLength = redis.pcall('GET', lenKey)
		local size = ARGV[1]
		if tonumber(bucketLength) >= tonumber(size) then
			return false
		end
		return true
	`)
	val, _ := isFreeScript.Run(context.Background(), getRedisClient(), []string{bucket.key}, bucket.size).Bool()
	return val
}

// Elements returns the values stored in the Redis List at _key_
func (bucket *BucketRedis) Elements() ([]string, error) {
	elements, err := getRedisClient().LRange(context.Background(), bucket.key, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("gostatix: error while fetching list values from redis, key: %s, error: %v", bucket.key, err)
	}
	return elements, nil
}

// NextSlot returns the next empty slot in the bucket starting from index 0
func (bucket *BucketRedis) NextSlot() (int64, error) {
	lPosArgs := redis.LPosArgs{Rank: 1, MaxLen: 0}
	pos, err := getRedisClient().LPos(context.Background(), bucket.key, "", lPosArgs).Result()
	if err != nil {
		return -1, fmt.Errorf("gostatix: error while fetching next empty slot: %v", err)
	}
	return pos, nil
}

// At returns the value stored at _index_ in the Redis List
func (bucket *BucketRedis) At(index uint64) (string, error) {
	val, err := getRedisClient().LIndex(context.Background(), bucket.key, int64(index)).Result()
	if err != nil {
		return "", fmt.Errorf("gostatix: error while fetching value at index: %v", err)
	}
	return val, nil
}

// Add inserts the _element_ in the bucket at the next available slot
func (bucket *BucketRedis) Add(element string) (bool, error) {
	if element == "" {
		return false, nil
	}
	addElement := redis.NewScript(`
		local key = KEYS[1]
		local lenKey = key .. '_len'
		local bucketLength = redis.pcall('GET', lenKey)
		local size = ARGV[2]
		if tonumber(bucketLength) >= tonumber(size) then
			return false
		end
		local element = ARGV[1]
		local pos = redis.pcall('LPOS', key, '')
		if pos == false then
			redis.pcall('LPUSH', key, element)
		else
			redis.pcall('LSET', key, tonumber(pos), element)
		end
		redis.pcall('INCRBY', lenKey, 1)
		return true
	`)
	val, err := addElement.Run(context.Background(), getRedisClient(), []string{bucket.key}, element, bucket.size).Bool()
	if err != nil {
		return false, fmt.Errorf("gostatix: error while adding element %s, error: %v", element, err)
	}
	if !val {
		return val, fmt.Errorf("bucket is full")
	}
	return true, nil
}

// Remove deletes the entry _element_ from the bucket
func (bucket *BucketRedis) Remove(element string) (bool, error) {
	removeElement := redis.NewScript(`
		local key = KEYS[1]
		local lenKey = key .. '_len'
		local element = ARGV[1]
		local pos = redis.call('LPOS', key, element)
		redis.call('LSET', key, pos, '')
		redis.pcall('INCRBY', lenKey, -1)
		return true
	`)
	_, err := removeElement.Run(context.Background(), getRedisClient(), []string{bucket.key}, element).Bool()
	if err != nil {
		return false, fmt.Errorf("gostatix: error while removing element %s, error: %v", element, err)
	}
	return true, nil
}

// Lookup returns true if the _element_ is present in the bucket, otherwise false
func (bucket *BucketRedis) Lookup(element string) (bool, error) {
	//Redis returns nil if an element doesn't exist in the list
	//While Golang Redis LPos command returns 0 for non-existent element inside the list with error set as "redis: nil"
	//This becomes confusing for the index of the first element in the list and non-existent values
	//below script handles the ambiguity
	exists := redis.NewScript(`
		local key = KEYS[1]
		local element = ARGV[1]
		local pos = redis.pcall('LPOS', key, element)
		if pos == false then
			return -1
		end
		return tonumber(pos)
	`)
	pos, err := exists.Run(context.Background(), getRedisClient(), []string{bucket.key}, element).Int64()
	if err != nil {
		return false, fmt.Errorf("gostatix: error while searching for %s, error: %v", element, err)
	}
	return pos > -1, nil
}

// Set inserts the _element_ at the specified _index_
func (bucket *BucketRedis) Set(index uint64, element string) error {
	_, err := getRedisClient().LSet(context.Background(), bucket.key, int64(index), element).Result()
	if err != nil {
		return fmt.Errorf("gostatix: error while setting element %s at index %d, error: %v", element, index, err)
	} else {
		return nil
	}
}

// UnSet removes the element stored at the specified _index_
func (bucket *BucketRedis) UnSet(index uint64) error {
	_, err := getRedisClient().LSet(context.Background(), bucket.key, int64(index), "").Result()
	if err != nil {
		return fmt.Errorf("gostatix: error while unsetting index %d, error: %v", index, err)
	} else {
		return nil
	}
}

// Equals checks if two BucketRedis are equal
func (bucket *BucketRedis) Equals(otherBucket *BucketRedis) (bool, error) {
	if bucket.size != otherBucket.size {
		return false, nil
	}
	equals := redis.NewScript(`
		local key1 = KEYS[1]
		local key2 = KEYS[2]
		local size = ARGV[1]
		local vals1 = redis.pcall('LRANGE', key1, 0, -1)
		local vals2 = redis.pcall('LRANGE', key2, 0, -1)
		for i=1, tonumber(size) do
			if vals1[i] ~= vals2[i] then
				return false
			end
		end
		return true
	`)
	ok, err := equals.Run(context.Background(), getRedisClient(), []string{bucket.key, otherBucket.key}, bucket.size).Bool()
	if err != nil {
		return false, fmt.Errorf("gostatix: error while comparing list %s with %s, error: %v", bucket.key, otherBucket.key, err)
	}
	return ok, nil
}

func (bucket *BucketRedis) incrLength() {
	getRedisClient().IncrBy(context.Background(), bucket.key+"_len", 0).Err()
}

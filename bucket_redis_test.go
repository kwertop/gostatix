package gostatix

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestBasicBucketRedis(t *testing.T) {
	initMockRedis()
	bucket := newBucketRedis("key", 10)
	initBucket("key", 10)
	bucket.add("foo")
	bucket.add("bar")
	bucket.add("baz")
	e, _ := bucket.at(0)
	if e != "foo" {
		t.Errorf("e should be %v", "foo")
	}
	e, _ = bucket.at(2)
	if e != "baz" {
		t.Errorf("e should be %v", "baz")
	}
	i, _ := bucket.nextSlot()
	if i != 3 {
		t.Error("next empty slot should be at 3")
	}
	bucket.remove("bar")
	e, _ = bucket.at(1)
	if e != "" {
		t.Error("e should be empty string")
	}
	bucket.set(1, "faz")
	ok, _ := bucket.lookup("faz")
	if !ok {
		t.Error("faz should be present in the bucket")
	}
	ok, _ = bucket.lookup("far")
	if ok {
		t.Error("far shouldn't be present in the bucket")
	}
}

func TestBucketRedisFull(t *testing.T) {
	initMockRedis()
	bucket := newBucketRedis("key", 4)
	initBucket("key", 4)
	bucket.add("foo")
	bucket.add("bar")
	bucket.add("baz")
	bucket.add("faz")
	ok, _ := bucket.add("far")
	if ok {
		t.Error("far shouldn't be added as bucket is full")
	}
}

func TestBucketRedisLength(t *testing.T) {
	initMockRedis()
	bucket := newBucketRedis("bkey", 10)
	initBucket("bkey", 10)
	bucket.add("foo")
	bucket.add("bar")
	bucket.add("baz")
	l := bucket.getLength()
	if l != 3 {
		t.Error("bucket length should be 3")
	}
	bucket.remove("foo")
	l = bucket.getLength()
	if l != 2 {
		t.Error("bucket length should be 2")
	}
}

func TestBucketRedisRemove(t *testing.T) {
	initMockRedis()
	bucket := newBucketRedis("rkey", 3)
	initBucket("rkey", 3)
	bucket.add("foo")
	bucket.add("bar")
	bucket.add("baz")
	ok1, _ := bucket.remove("foo")
	ok2, _ := bucket.remove("foo")
	if !ok1 {
		t.Error("foo should be removed as it's present in bucket")
	}
	if ok2 {
		t.Error("can't remove foo as it isn't in the bucket")
	}
}

func TestBucketRedisEquals(t *testing.T) {
	initMockRedis()
	b1 := newBucketRedis("key1", 10)
	initBucket("key1", 10)
	b1.add("foo")
	b1.add("bar")
	b1.add("baz")
	b2 := newBucketRedis("key2", 10)
	initBucket("key2", 10)
	b2.add("foo")
	b2.add("bar")
	b2.add("baz")
	ok, _ := b1.equals(b2)
	if !ok {
		t.Error("b1 and b2 should be equal")
	}
	b2.remove("foo")
	ok, _ = b1.equals(b2)
	if ok {
		t.Error("b1 and b2 shouldn't be equal here")
	}
}

func initBucket(key string, size uint) error {
	script := redis.NewScript(`
		local key = KEYS[1]
		local size = ARGV[1]
		for i=1, tonumber(size) do
			redis.call('LPUSH', key, '')
		end
		return true
	`)
	err := script.Run(context.Background(), getRedisClient(), []string{key}, size).Err()
	return err
}

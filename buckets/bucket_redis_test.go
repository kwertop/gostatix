package buckets

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/kwertop/gostatix"
	"github.com/redis/go-redis/v9"
)

func TestBasicBucketRedis(t *testing.T) {
	initMockRedis()
	bucket := NewBucketRedis("key", 10)
	initBucket("key", 10)
	bucket.Add("foo")
	bucket.Add("bar")
	bucket.Add("baz")
	e, _ := bucket.At(0)
	if e != "foo" {
		t.Errorf("e should be %v", "foo")
	}
	e, _ = bucket.At(2)
	if e != "baz" {
		t.Errorf("e should be %v", "baz")
	}
	i, _ := bucket.NextSlot()
	if i != 3 {
		t.Error("next empty slot should be at 3")
	}
	bucket.Remove("bar")
	e, _ = bucket.At(1)
	if e != "" {
		t.Error("e should be empty string")
	}
	bucket.Set(1, "faz")
	ok, _ := bucket.Lookup("faz")
	if !ok {
		t.Error("faz should be present in the bucket")
	}
	ok, _ = bucket.Lookup("far")
	if ok {
		t.Error("far shouldn't be present in the bucket")
	}
}

func TestBucketRedisFull(t *testing.T) {
	initMockRedis()
	bucket := NewBucketRedis("key", 4)
	initBucket("key", 4)
	bucket.Add("foo")
	bucket.Add("bar")
	bucket.Add("baz")
	bucket.Add("faz")
	ok, _ := bucket.Add("far")
	if ok {
		t.Error("far shouldn't be added as bucket is full")
	}
}

func TestBucketRedisLength(t *testing.T) {
	initMockRedis()
	bucket := NewBucketRedis("key", 10)
	initBucket("key", 10)
	bucket.Add("foo")
	bucket.Add("bar")
	bucket.Add("baz")
	l := bucket.Length()
	if l != 3 {
		t.Error("bucket length should be 3")
	}
	bucket.Remove("foo")
	l = bucket.Length()
	if l != 2 {
		t.Error("bucket length should be 2")
	}
}

func TestBucketRedisRemove(t *testing.T) {
	initMockRedis()
	bucket := NewBucketRedis("key", 3)
	initBucket("key", 3)
	bucket.Add("foo")
	bucket.Add("bar")
	bucket.Add("baz")
	ok1, _ := bucket.Remove("foo")
	ok2, _ := bucket.Remove("foo")
	if !ok1 {
		t.Error("foo should be removed as it's present in bucket")
	}
	if ok2 {
		t.Error("can't remove foo as it isn't in the bucket")
	}
}

func TestBucketRedisEquals(t *testing.T) {
	initMockRedis()
	b1 := NewBucketRedis("key1", 10)
	initBucket("key1", 10)
	b1.Add("foo")
	b1.Add("bar")
	b1.Add("baz")
	b2 := NewBucketRedis("key2", 10)
	initBucket("key2", 10)
	b2.Add("foo")
	b2.Add("bar")
	b2.Add("baz")
	ok, _ := b1.Equals(b2)
	if !ok {
		t.Error("b1 and b2 should be equal")
	}
	b2.Remove("foo")
	ok, _ = b1.Equals(b2)
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
	err := script.Run(context.Background(), gostatix.GetRedisClient(), []string{key}, size).Err()
	return err
}

func initMockRedis() {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := gostatix.ParseRedisURI(redisUri)
	gostatix.MakeRedisClient(*connOptions)
}

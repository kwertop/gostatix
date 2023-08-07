package filters

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/kwertop/gostatix"
)

func TestCuckooRedisBasic(t *testing.T) {
	initMockRedis()
	filter, _ := NewCuckooFilterRedisWithErrorRate(20, 4, 500, 0.01)
	filter.Insert([]byte("john"), false)
	filter.Insert([]byte("jane"), false)
	if filter.length != 2 {
		t.Errorf("filter length should be 2, instead found %v", filter.length)
	}
	bucketsLength := 0
	for b := range filter.buckets {
		bucketsLength += int(filter.buckets[b].Length())
	}
	if bucketsLength != 2 {
		t.Errorf("total elements insisde buckets should be 2, instead found %v", bucketsLength)
	}
}

func TestCuckooRedisAddDifferentBuckets(t *testing.T) {
	initMockRedis()
	filter, _ := NewCuckooFilterRedisWithErrorRate(20, 2, 500, 0.01)
	e := []byte("foo")
	filter.Insert(e, false)
	filter.Insert(e, false)
	filter.Insert(e, false)
	filter.Insert(e, false)
	_, fIndex, sIndex, _ := filter.getPositions(e)
	firstIndex := filter.getIndexKey(fIndex)
	secondIndex := filter.getIndexKey(sIndex)
	if filter.buckets[firstIndex].IsFree() || filter.buckets[secondIndex].IsFree() {
		t.Error("both buckets should be full")
	}
	if filter.length != 4 {
		t.Errorf("filter length should be 4, instead found %v", filter.length)
	}
	bucketsLength := 0
	for b := range filter.buckets {
		bucketsLength += int(filter.buckets[b].Length())
	}
	if bucketsLength != 4 {
		t.Errorf("total elements insisde buckets should be 4, instead found %v", bucketsLength)
	}
}

func TestCuckooRedisRetries(t *testing.T) {
	initMockRedis()
	filter, _ := NewCuckooFilterRedisWithRetries(10, 1, 3, 1)
	e := []byte("foo")
	fingerPrint, fIndex, sIndex, _ := filter.getPositions(e)
	firstIndex := filter.getIndexKey(fIndex)
	secondIndex := filter.getIndexKey(sIndex)
	filter.buckets[firstIndex].Add("bar")
	filter.buckets[secondIndex].Add("baz")
	filter.length += 2
	ok := filter.Insert(e, false)
	if !ok {
		t.Errorf("%v should get added in the filter", string(e))
	}
	bucketsLength := 0
	for b := range filter.buckets {
		bucket := filter.buckets[b]
		if bucket.Length() > 0 {
			elem, _ := bucket.At(0)
			if elem != "bar" && elem != "baz" && elem != fingerPrint {
				t.Errorf("elem shuold be either \"bar\", \"baz\" or \"%s\", instead found %v", fingerPrint, elem)
			}
		}
		bucketsLength += int(bucket.Length())
	}
	if filter.length != 3 {
		t.Errorf("filter length should be 3, instead found %v", filter.length)
	}
	if bucketsLength != 3 {
		t.Errorf("total elements insisde buckets should be 3, instead found %v", bucketsLength)
	}
}

func TestCuckooFilterRedisFull(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("filter should be full, and panic should occur")
		}
	}()

	initMockRedis()
	filter, _ := NewCuckooFilterRedis(1, 1, 3)
	e := []byte("foo")
	filter.Insert(e, false)
	filter.Insert(e, false)
}

func TestCuckooRedisInsertAndLookup(t *testing.T) {
	initMockRedis()
	filter, _ := NewCuckooFilterRedisWithErrorRate(20, 4, 500, 0.01)
	filter.Insert([]byte("alice"), false)
	filter.Insert([]byte("andrew"), false)
	filter.Insert([]byte("bob"), false)
	filter.Insert([]byte("sam"), false)

	filter.Insert([]byte("alice"), false)
	filter.Insert([]byte("andrew"), false)
	filter.Insert([]byte("bob"), false)
	filter.Insert([]byte("sam"), false)

	ok1, _ := filter.Lookup([]byte("samx"))
	ok2, _ := filter.Lookup([]byte("samy"))
	ok3, e1 := filter.Lookup([]byte("alice"))
	ok4, _ := filter.Lookup([]byte("joe"))

	if ok1 {
		t.Error("samx shouldn't be present in filter")
	}
	if ok2 {
		t.Error("samy shouldn't be present in filter")
	}
	if !ok3 {
		t.Errorf("alice should be present in filter, error: %v", e1)
	}
	if ok4 {
		t.Error("joe shouldn't be present in filter")
	}
}

func TestRemovePresentCuckooRedis(t *testing.T) {
	initMockRedis()
	filter, _ := NewCuckooFilterRedisWithErrorRate(20, 4, 500, 0.01)
	e1 := []byte("foo")
	e2 := []byte("bar")
	filter.Insert(e1, false)
	filter.Insert(e2, false)
	ok, _ := filter.Remove([]byte("foo"))
	if !ok {
		t.Error("should be able to remove as e1 is in the filter")
	}
	ok, _ = filter.Remove([]byte("foo"))
	if ok {
		t.Error("shouldn't be able to remove as e1 isn't in the filter")
	}
}

func initMockRedis() {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := gostatix.ParseRedisURI(redisUri)
	gostatix.MakeRedisClient(*connOptions)
}

package gostatix

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestCuckooRedisBasic(t *testing.T) {
	initMockRedis()
	filter, _ := NewCuckooFilterRedisWithErrorRate(20, 4, 500, 0.01)
	filter.Insert([]byte("john"), false)
	filter.Insert([]byte("jane"), false)
	filterLength := filter.Length()
	if filterLength != 2 {
		t.Errorf("filter length should be 2, instead found %v", filterLength)
	}
	bucketsLength := 0
	for b := range filter.buckets {
		bucketsLength += int(filter.buckets[b].getLength())
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
	if filter.buckets[firstIndex].isFree() || filter.buckets[secondIndex].isFree() {
		t.Error("both buckets should be full")
	}
	filterLength := filter.Length()
	if filterLength != 4 {
		t.Errorf("filter length should be 4, instead found %v", filterLength)
	}
	bucketsLength := 0
	for b := range filter.buckets {
		bucketsLength += int(filter.buckets[b].getLength())
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
	filter.buckets[firstIndex].add("bar")
	filter.buckets[secondIndex].add("baz")
	filter.incrLength()
	filter.incrLength()
	ok := filter.Insert(e, false)
	if !ok {
		t.Errorf("%v should get added in the filter", string(e))
	}
	bucketsLength := 0
	for b := range filter.buckets {
		bucket := filter.buckets[b]
		if bucket.getLength() > 0 {
			elem, _ := bucket.at(0)
			if elem != "bar" && elem != "baz" && elem != fingerPrint {
				t.Errorf("elem shuold be either \"bar\", \"baz\" or \"%s\", instead found %v", fingerPrint, elem)
			}
		}
		bucketsLength += int(bucket.getLength())
	}
	filterLength := filter.Length()
	if filterLength != 3 {
		t.Errorf("filter length should be 3, instead found %v", filterLength)
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

func TestRemoveNotPresentCuckooRedis(t *testing.T) {
	initMockRedis()
	filter, _ := NewCuckooFilterRedisWithErrorRate(20, 4, 500, 0.01)
	e1 := []byte("foo")
	filter.Insert(e1, false)
	ok, _ := filter.Remove([]byte("bar"))
	if ok {
		t.Error("shouldn't be able to remove as \"bar\" isn't in the filter")
	}
}

func TestRollbackWhenFullCuckooRedis(t *testing.T) {
	initMockRedis()
	filter, _ := NewCuckooFilterRedis(5, 1, 3)
	ok := filter.Insert([]byte("one"), false)
	if !ok {
		t.Error("should insert one")
	}
	ok = filter.Insert([]byte("two"), false)
	if !ok {
		t.Error("should insert two")
	}
	ok = filter.Insert([]byte("three"), false)
	if !ok {
		t.Error("should insert three")
	}
	ok = filter.Insert([]byte("four"), false)
	if !ok {
		t.Error("should insert four")
	}
	ok = filter.Insert([]byte("five"), false)
	if !ok {
		t.Error("should insert five")
	}
	snapshot1, _ := filter.Export()

	defer func() {
		r := recover()
		if r == nil {
			t.Error("filter should be full, and panic should occur")
		}
		snapshot2, _ := filter.Export()
		if !reflect.DeepEqual(snapshot1, snapshot2) {
			t.Error("snapshot1 and snapshot2 should be equal")
		}
	}()

	ok = filter.Insert([]byte("six"), false)
	if !ok {
		t.Error("should insert six")
	}
}

func TestNoRollbackWhenFullCuckooRedis(t *testing.T) {
	initMockRedis()
	filter, _ := NewCuckooFilterRedis(5, 1, 3)
	ok := filter.Insert([]byte("one"), false)
	if !ok {
		t.Error("should insert one")
	}
	ok = filter.Insert([]byte("two"), false)
	if !ok {
		t.Error("should insert two")
	}
	ok = filter.Insert([]byte("three"), false)
	if !ok {
		t.Error("should insert three")
	}
	ok = filter.Insert([]byte("four"), false)
	if !ok {
		t.Error("should insert four")
	}
	ok = filter.Insert([]byte("five"), false)
	if !ok {
		t.Error("should insert five")
	}
	snapshot1, _ := filter.Export()

	defer func() {
		r := recover()
		if r == nil {
			t.Error("filter should be full, and panic should occur")
		}
		snapshot2, _ := filter.Export()
		if reflect.DeepEqual(snapshot1, snapshot2) {
			t.Error("snapshot1 and snapshot2 shouldn't be equal")
		}
	}()

	ok = filter.Insert([]byte("six"), true)
	if !ok {
		t.Error("should insert six")
	}
}

func TestCuckooImportInvalidJSONCuckooRedis(t *testing.T) {
	data := []byte("{invalid}")

	var g CuckooFilterRedis
	err := g.Import(data, false)
	if err == nil {
		t.Error("expected error while unmarshalling invalid data")
	}
}

func TestCuckooEqualsCuckooRedis(t *testing.T) {
	initMockRedis()
	filter1, _ := NewCuckooFilterRedis(5, 1, 3)
	filter1.Insert([]byte("one"), false)
	filter1.Insert([]byte("two"), false)
	filter1.Insert([]byte("three"), false)
	filter2, _ := NewCuckooFilterRedis(5, 1, 3)
	filter2.Insert([]byte("one"), false)
	filter2.Insert([]byte("two"), false)
	filter2.Insert([]byte("three"), false)
	if ok, _ := filter1.Equals(*filter2); !ok {
		t.Error("filter1 and filter2 should be same")
	}
}

func TestCuckooMarshalUnmarshalCuckooRedis(t *testing.T) {
	initMockRedis()
	filter1, _ := NewCuckooFilterRedis(5, 1, 3)
	filter1.Insert([]byte("one"), false)
	filter1.Insert([]byte("two"), false)
	filter1.Insert([]byte("three"), false)
	filter1.Insert([]byte("four"), false)
	snapshot, _ := filter1.Export()
	filter2, _ := NewCuckooFilterRedis(0, 0, 0)
	filter2.Import(snapshot, true)
	ok, _ := filter2.Lookup([]byte("one"))
	if !ok {
		t.Error("\"one\" should be in filter3")
	}
	ok, _ = filter2.Lookup([]byte("five"))
	if ok {
		t.Error("\"five\" should not be in filter3")
	}
	ok, _ = filter1.Equals(*filter2)
	if !ok {
		t.Errorf("filter1 and filter2 should be same")
	}
}

func TestCuckooRedisImportFromRedisKey(t *testing.T) {
	initMockRedis()
	filter1, _ := NewCuckooFilterRedis(5, 1, 3)
	filter1.Insert([]byte("one"), false)
	filter1.Insert([]byte("two"), false)
	filter1.Insert([]byte("three"), false)
	filter1.Insert([]byte("four"), false)

	metadataKey := filter1.MetadataKey()
	filter2, _ := NewCuckooFilterRedisFromKey(metadataKey)
	ok, _ := filter2.Lookup([]byte("one"))
	if !ok {
		t.Error("\"one\" should be in filter3")
	}
	ok, _ = filter2.Lookup([]byte("five"))
	if ok {
		t.Error("\"five\" should not be in filter3")
	}
	ok, _ = filter1.Equals(*filter2)
	if !ok {
		t.Errorf("filter1 and filter2 should be same")
	}
}

func BenchmarkCuckooRedisInsert10MX4X500X001(b *testing.B) {
	b.StopTimer()
	connOpts, _ := ParseRedisURI("redis://127.0.0.1:6379")
	MakeRedisClient(*connOpts)
	filter, err := NewCuckooFilterRedisWithErrorRate(1000*1000, 4, 500, 0.001)
	if err != nil {
		fmt.Printf("err: %v", err)
		return
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		filter.Insert([]byte(strconv.FormatUint(rand.Uint64(), 10)), true)
	}
}

func BenchmarkCuckooRedisLookup1MX4X500X001X10k(b *testing.B) {
	b.StopTimer()
	connOpts, _ := ParseRedisURI("redis://127.0.0.1:6379")
	MakeRedisClient(*connOpts)
	filter, _ := NewCuckooFilterRedisWithErrorRate(1000*1000, 4, 500, 0.001)
	for i := 0; i < 10000; i++ {
		filter.Insert([]byte(strconv.FormatUint(rand.Uint64(), 10)), true)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		filter.Lookup([]byte(strconv.FormatUint(rand.Uint64(), 10)))
	}
}

func BenchmarkCuckooRedisLookup100MX4X500X001X1M(b *testing.B) {
	b.StopTimer()
	connOpts, _ := ParseRedisURI("redis://127.0.0.1:6379")
	connOpts.ReadTimeout = time.Hour
	connOpts.WriteTimeout = time.Hour
	MakeRedisClient(*connOpts)
	filter, _ := NewCuckooFilterRedisWithErrorRate(100*1000*1000, 4, 500, 0.001)
	for i := 0; i < 1000000; i++ {
		filter.Insert([]byte(strconv.FormatUint(rand.Uint64(), 10)), true)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		filter.Lookup([]byte(strconv.FormatUint(rand.Uint64(), 10)))
	}
}

package count

import (
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/kwertop/gostatix"
)

func TestTopKRedisBasic(t *testing.T) {
	initMockRedis()
	k := uint(5)
	errorRate := 0.001
	delta := 0.999
	topkSingleEntry := NewTopKRedis(k, errorRate, delta)

	frequencyMap := make(map[string]int)

	for i := range items {
		topkSingleEntry.Insert([]byte(items[i]), 1)
		frequencyMap[items[i]]++
	}

	topkBatchEntry := NewTopKRedis(k, errorRate, delta)
	for key, val := range frequencyMap {
		topkBatchEntry.Insert([]byte(key), uint64(val))
	}

	val1, _ := topkSingleEntry.Values()
	val2, _ := topkBatchEntry.Values()

	if !reflect.DeepEqual(val1, val2) {
		t.Error("both topk data structures should be equal")
	}

	for i := range val1 {
		if val1[i].count != uint64(frequencyMap[val1[i].element]) {
			t.Errorf("frequency doesn't match for %s. Instead found %d and %d", val1[i].element, val1[i].count, frequencyMap[val1[i].element])
		}
	}
}

func TestTopRedisKDifferentKs(t *testing.T) {
	initMockRedis()
	errorRate := 0.001
	delta := 0.999
	topk := NewTopKRedis(11, errorRate, delta)

	frequencyMap := make(map[string]int)

	for i := range items {
		topk.Insert([]byte(items[i]), 1)
		frequencyMap[items[i]]++
	}

	val, _ := topk.Values()

	for i := range expectedTopElements {
		if strings.Compare(expectedTopElements[i], val[i].element) != 0 {
			t.Errorf("values at position %d don't match", i)
		}
		if val[i].count != uint64(frequencyMap[val[i].element]) {
			t.Errorf("frequency doesn't match for %s. Instead found %d and %d", val[i].element, val[i].count, frequencyMap[val[i].element])
		}
	}

	topk = NewTopKRedis(6, errorRate, delta)
	for i := range items {
		topk.Insert([]byte(items[i]), 1)
	}

	val, _ = topk.Values()

	for i := 0; i < 6; i++ {
		if strings.Compare(expectedTopElements[i], val[i].element) != 0 {
			t.Errorf("values at position %d don't match", i)
		}
		if val[i].count != uint64(frequencyMap[val[i].element]) {
			t.Errorf("frequency doesn't match for %s. Instead found %d and %d", val[i].element, val[i].count, frequencyMap[val[i].element])
		}
	}
}

func TestTopKRedisEquals(t *testing.T) {
	initMockRedis()
	errorRate := 0.001
	delta := 0.999

	k := NewTopKRedis(10, errorRate, delta)
	for i := 0; i < 10; i++ {
		k.Insert([]byte(items[i]), 1)
	}

	l := NewTopKRedis(10, errorRate, delta)
	for i := 0; i < 10; i++ {
		l.Insert([]byte(items[i]), 1)
	}

	if ok, _ := l.Equals(k); !ok {
		t.Errorf("topk k and l should be equal")
	}
}

func TestTopKRedisImportExport(t *testing.T) {
	initMockRedis()
	errorRate := 0.1
	delta := 0.9

	k := NewTopKRedis(5, errorRate, delta)
	for i := 0; i < 10; i++ {
		k.Insert([]byte(items[i]), 1)
	}

	l := NewTopKRedis(5, errorRate, delta)
	for i := 0; i < 10; i++ {
		l.Insert([]byte(items[i]), 1)
	}

	s, _ := k.Export()

	m := NewTopKRedis(10, errorRate, delta)
	m.Import(s, true)

	if ok, _ := m.Equals(k); !ok {
		t.Errorf("topk k and m should be equal")
	}
}

func BenchmarkTopKRedisInsert100X1M(b *testing.B) {
	b.StopTimer()
	connOpts, _ := gostatix.ParseRedisURI("redis://127.0.0.1:6379")
	gostatix.MakeRedisClient(*connOpts)
	topk := NewTopKRedis(100, 0.001, 0.999)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		topk.Insert([]byte(strconv.FormatUint(rand.Uint64(), 10)), 1)
	}
}

func BenchmarkTopKRedisValues100X1M(b *testing.B) {
	b.StopTimer()
	connOpts, _ := gostatix.ParseRedisURI("redis://127.0.0.1:6379")
	gostatix.MakeRedisClient(*connOpts)
	topk := NewTopKRedis(100, 0.001, 0.999)
	for i := 0; i < 1000000; i++ {
		topk.Insert([]byte(strconv.FormatUint(rand.Uint64(), 10)), 1)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		topk.Values()
	}
}

func BenchmarkTopKRedisValues10kX1M(b *testing.B) {
	b.StopTimer()
	connOpts, _ := gostatix.ParseRedisURI("redis://127.0.0.1:6379")
	gostatix.MakeRedisClient(*connOpts)
	topk := NewTopKRedis(1000, 0.0001, 0.9999)
	for i := 0; i < 100000; i++ {
		topk.Insert([]byte(strconv.FormatUint(rand.Uint64(), 10)), 1)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		topk.Values()
	}
}

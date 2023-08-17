package count

import (
	"bytes"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var items = []string{
	"apple",
	"orange",
	"banana",
	"carrot",
	"apple",
	"grape",
	"apple",
	"carrot",
	"apple",
	"banana",
	"plum",
	"plum",
	"peach",
	"apple",
	"carrot",
	"peach",
	"mango",
	"apple",
	"grape",
	"melon",
	"pineapple",
	"kiwi",
	"banana",
	"grape",
	"apple",
	"kiwi",
	"pineapple",
	"mango",
	"plum",
	"peach",
	"banana",
}

var expectedTopElements = []string{
	"apple",
	"banana",
	"carrot",
	"grape",
	"peach",
	"plum",
	"kiwi",
	"mango",
	"pineapple",
	"melon",
	"orange",
}

func TestTopKBasic(t *testing.T) {
	k := uint(11)
	errorRate := 0.001
	delta := 0.999
	topkSingleEntry := NewTopK(k, errorRate, delta)

	frequencyMap := make(map[string]int)

	for i := range items {
		topkSingleEntry.Insert([]byte(items[i]), 1)
		frequencyMap[items[i]]++
	}

	topkBatchEntry := NewTopK(k, errorRate, delta)
	for key, val := range frequencyMap {
		topkBatchEntry.Insert([]byte(key), uint64(val))
	}

	val1 := topkSingleEntry.Values()
	val2 := topkBatchEntry.Values()

	if !reflect.DeepEqual(val1, val2) {
		t.Error("both topk data structures should be equal")
	}

	for i := range val1 {
		if val1[i].count != uint64(frequencyMap[val1[i].element]) {
			t.Errorf("frequency doesn't match for %s. Instead found %d and %d", val1[i].element, val1[i].count, frequencyMap[val1[i].element])
		}
	}
}

func TestTopKDifferentKs(t *testing.T) {
	errorRate := 0.001
	delta := 0.999
	topk := NewTopK(15, errorRate, delta)

	frequencyMap := make(map[string]int)

	for i := range items {
		topk.Insert([]byte(items[i]), 1)
		frequencyMap[items[i]]++
	}

	val := topk.Values()

	for i := range expectedTopElements {
		if strings.Compare(expectedTopElements[i], val[i].element) != 0 {
			t.Errorf("values at position %d don't match", i)
		}
		if val[i].count != uint64(frequencyMap[val[i].element]) {
			t.Errorf("frequency doesn't match for %s. Instead found %d and %d", val[i].element, val[i].count, frequencyMap[val[i].element])
		}
	}

	topk = NewTopK(3, errorRate, delta)
	for i := range items {
		topk.Insert([]byte(items[i]), 1)
	}

	val = topk.Values()

	for i := 0; i < 3; i++ {
		if strings.Compare(expectedTopElements[i], val[i].element) != 0 {
			t.Errorf("values at position %d don't match", i)
		}
		if val[i].count != uint64(frequencyMap[val[i].element]) {
			t.Errorf("frequency doesn't match for %s. Instead found %d and %d", val[i].element, val[i].count, frequencyMap[val[i].element])
		}
	}
}

func TestEquals(t *testing.T) {
	errorRate := 0.001
	delta := 0.999

	k := NewTopK(10, errorRate, delta)
	for i := 0; i < 10; i++ {
		k.Insert([]byte(items[i]), 1)
	}

	l := NewTopK(10, errorRate, delta)
	for i := 0; i < 10; i++ {
		l.Insert([]byte(items[i]), 1)
	}

	if ok, _ := l.Equals(k); !ok {
		t.Errorf("topk k and l should be equal")
	}
}

func TestTopKImportExport(t *testing.T) {
	errorRate := 0.001
	delta := 0.999

	k := NewTopK(5, errorRate, delta)
	for i := 0; i < 10; i++ {
		k.Insert([]byte(items[i]), 1)
	}

	l := NewTopK(5, errorRate, delta)
	for i := 0; i < 10; i++ {
		l.Insert([]byte(items[i]), 1)
	}

	s, _ := k.Export()
	u, _ := l.Export()

	if !reflect.DeepEqual(s, u) {
		t.Errorf("topk l and k should be equal")
	}

	m := NewTopK(10, errorRate, delta)
	m.Import(s)

	if ok, _ := m.Equals(k); !ok {
		t.Errorf("topk k and m should be equal")
	}
}

func TestTopKBinaryReadWrite(t *testing.T) {
	errorRate := 0.001
	delta := 0.999

	k := NewTopK(5, errorRate, delta)
	for i := 0; i < 10; i++ {
		k.Insert([]byte(items[i]), 1)
	}

	var buff bytes.Buffer
	_, err := k.WriteTo(&buff)
	if err != nil {
		t.Error("should not error out writing to buffer")
	}

	l := &TopK{}
	_, err = l.ReadFrom(&buff)
	if err != nil {
		t.Error("should not error out writing to buffer")
	}

	if ok, _ := k.Equals(l); !ok {
		t.Error("k and l should be equal")
	}
}

func BenchmarkTopKInsert100X1M(b *testing.B) {
	b.StopTimer()
	topk := NewTopK(100, 0.001, 0.999)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		topk.Insert([]byte(strconv.FormatUint(rand.Uint64(), 10)), 1)
	}
}

func BenchmarkTopKValues100X1M(b *testing.B) {
	b.StopTimer()
	topk := NewTopK(100, 0.001, 0.999)
	for i := 0; i < 1000000; i++ {
		topk.Insert([]byte(strconv.FormatUint(rand.Uint64(), 10)), 1)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		topk.Values()
	}
}

func BenchmarkTopKValues10kX1M(b *testing.B) {
	b.StopTimer()
	topk := NewTopK(10000, 0.0001, 0.9999)
	for i := 0; i < 10000000; i++ {
		topk.Insert([]byte(strconv.FormatUint(rand.Uint64(), 10)), 1)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		topk.Values()
	}
}

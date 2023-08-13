package count

import (
	"reflect"
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
	k := uint(5)
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

}

package filters

import "testing"

func TestCuckooFilterBasic(t *testing.T) {
	filter := NewCuckooFilterWithErrorRate(20, 4, 500, 0.01)
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

package count

import (
	"math"
	"strconv"
	"testing"
)

func TestHyperLogLogRedis(t *testing.T) {
	initMockRedis()
	numDistinct := 100
	h, _ := NewHyperLogLogRedis(128)
	for i := 0; i < 1000; i++ {
		data := []byte(strconv.FormatInt(int64(i), 10))
		h.Update(data)
	}
	distinctVals, _ := h.Count(true, true)
	if math.Abs(float64(distinctVals)-float64(numDistinct)) > 2 {
		t.Errorf("too much variance in calculated distinct values; got %d, exact %d", distinctVals, numDistinct)
	}
}

func TestHyperLogLogRedisMerge(t *testing.T) {
	initMockRedis()
	f, _ := NewHyperLogLogRedis(16)
	g, _ := NewHyperLogLogRedis(16)
	h, _ := NewHyperLogLogRedis(16)
	i, _ := NewHyperLogLogRedis(16)

	f.Update([]byte("foo"))
	f.Update([]byte("bar"))

	g.Update([]byte("abc"))
	g.Update([]byte("xyz"))

	h.Merge(g)
	h.Merge(f)

	i.Merge(f)
	i.Merge(g)

	ok, _ := h.Equals(i)
	if !ok {
		t.Errorf("h and i should be equal")
	}
}

func TestHyperLogLogRedisEquals(t *testing.T) {
	initMockRedis()
	f, _ := NewHyperLogLogRedis(32)
	g, _ := NewHyperLogLogRedis(16)
	h, _ := NewHyperLogLogRedis(16)

	h.Update([]byte("john"))
	h.Update([]byte("jane"))

	g.Update([]byte("john"))
	g.Update([]byte("jane"))

	ok1, _ := f.Equals(g)
	ok2, _ := f.Equals(h)
	if ok1 || ok2 {
		t.Errorf("f is neither equal to g nor h")
	}

	ok3, _ := h.Equals(g)
	if !ok3 {
		t.Errorf("h and g should be equal")
	}

	g.Update([]byte("alice"))

	ok3, _ = h.Equals(g)
	if ok3 {
		t.Errorf("h and g shouldn't be equal")
	}
}

func TestHyperLogLogRedisImportExport(t *testing.T) {
	initMockRedis()
	g, _ := NewHyperLogLogRedis(16)
	h, _ := NewHyperLogLogRedis(16)

	h.Update([]byte("foo"))
	h.Update([]byte("bar"))
	h.Update([]byte("baz"))

	g.Update([]byte("foo"))
	g.Update([]byte("bar"))
	g.Update([]byte("baz"))

	s2, _ := g.Export()

	f, _ := NewHyperLogLogRedis(16)
	f.Import(s2, true)

	ok1, _ := g.Equals(f)
	ok2, _ := h.Equals(f)
	if !ok1 || !ok2 {
		t.Errorf("h, g and f should be same")
	}
}

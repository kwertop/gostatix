package count

import (
	"bytes"
	"math"
	"reflect"
	"strconv"
	"testing"

	"github.com/kwertop/gostatix"
)

func TestHyperLogLog(t *testing.T) {
	numDistinct := 100
	h, _ := NewHyperLogLog(128)
	for i := 0; i < 1000; i++ {
		data := []byte(strconv.FormatInt(int64(i), 10))
		h.Update(data)
	}
	distinctVals := h.Count(true, true)
	if math.Abs(float64(distinctVals)-float64(numDistinct)) > 2 {
		t.Errorf("too much variance in calculated distinct values; got %d, exact %d", distinctVals, numDistinct)
	}
}

func TestHyperLogLogMerge(t *testing.T) {
	f, _ := NewHyperLogLog(16)
	g, _ := NewHyperLogLog(16)
	h, _ := NewHyperLogLog(16)

	f.Update([]byte("foo"))
	f.Update([]byte("bar"))

	g.Update([]byte("abc"))
	g.Update([]byte("xyz"))

	h.Merge(g)
	h.Merge(f)

	for i := range h.registers {
		if h.registers[i] != uint8(gostatix.Max(uint(f.registers[i]), uint(g.registers[i]))) {
			t.Error("value doesn't match expected")
		}
	}
}

func TestHyperLogLogEquals(t *testing.T) {
	f, _ := NewHyperLogLog(32)
	g, _ := NewHyperLogLog(16)
	h, _ := NewHyperLogLog(16)

	h.Update([]byte("john"))
	h.Update([]byte("jane"))

	g.Update([]byte("john"))
	g.Update([]byte("jane"))

	if f.Equals(g) || f.Equals(h) {
		t.Errorf("f is neither equal to g nor h")
	}

	if !h.Equals(g) {
		t.Errorf("h and g should be equal")
	}

	g.Update([]byte("alice"))

	if h.Equals(g) {
		t.Errorf("h and g shouldn't be equal")
	}
}

func TestHyperLogLogImportExport(t *testing.T) {
	g, _ := NewHyperLogLog(16)
	h, _ := NewHyperLogLog(16)

	h.Update([]byte("foo"))
	h.Update([]byte("bar"))
	h.Update([]byte("baz"))

	g.Update([]byte("foo"))
	g.Update([]byte("bar"))
	g.Update([]byte("baz"))

	s1, _ := h.Export()
	s2, _ := g.Export()

	if !reflect.DeepEqual(s1, s2) {
		t.Errorf("exported value of h and g should be equal")
	}

	f, _ := NewHyperLogLog(16)
	f.Import(s2)

	if !g.Equals(f) || !h.Equals(f) {
		t.Errorf("h, g and f should be same")
	}
}

func TestHyperLogLogBinaryReadWrite(t *testing.T) {
	g, _ := NewHyperLogLog(16)

	g.Update([]byte("foo"))
	g.Update([]byte("bar"))
	g.Update([]byte("baz"))

	var buff bytes.Buffer
	_, err := g.WriteTo(&buff)
	if err != nil {
		t.Error("should not error out while writing to buffer")
	}

	h, _ := NewHyperLogLog(2)
	_, err = h.ReadFrom(&buff)
	if err != nil {
		t.Error("should not error out while reading from buffer")
	}

	if !g.Equals(h) {
		t.Error("g and h should be equal")
	}
}

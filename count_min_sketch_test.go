package gostatix

import (
	"bytes"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
)

const delta = 0.999

func TestCountMinSketchBasic(t *testing.T) {
	cms, _ := NewCountMinSketchFromEstimates(0.001, delta)
	e1 := []byte("foo")
	e2 := []byte("bar")
	e3 := []byte("baz")
	cms.UpdateOnce(e1)
	cms.UpdateOnce(e1)
	cms.UpdateOnce(e2)
	c1 := cms.Count(e1)
	c2 := cms.Count(e2)
	c3 := cms.Count(e3)
	if c1 != 2 {
		t.Errorf("count of e1 should be 2, found %d", c1)
	}
	if c2 != 1 {
		t.Errorf("count of e2 should be 1, found %d", c2)
	}
	if c3 != 0 {
		t.Errorf("count of e3 should be 0, found %d", c3)
	}
}

func TestCountMinSketchMerge(t *testing.T) {
	cms1, _ := NewCountMinSketchFromEstimates(0.001, delta)
	cms2, _ := NewCountMinSketchFromEstimates(0.001, delta)

	cms1.UpdateString("foo", 1)
	cms1.UpdateString("foo", 1)
	cms1.UpdateString("foo", 1)
	cms1.UpdateString("baz", 1)

	cms2.UpdateString("foo", 1)
	cms2.UpdateString("bar", 1)
	cms2.UpdateString("bar", 1)
	cms2.UpdateString("baz", 1)

	cms1.Merge(cms2)

	ok1 := cms1.CountString("foo")
	ok2 := cms1.CountString("bar")
	ok3 := cms1.CountString("baz")
	ok4 := cms1.CountString("faz")

	if ok1 != 4 {
		t.Errorf("count of \"foo\" should be 4, found %d", ok1)
	}
	if ok2 != 2 {
		t.Errorf("count of \"bar\" should be 2, found %d", ok2)
	}
	if ok3 != 2 {
		t.Errorf("count of \"baz\" should be 2, found %d", ok3)
	}
	if ok4 != 0 {
		t.Errorf("count of \"faz\" should be 0, found %d", ok4)
	}
}

func TestCountMinSketchMergeError(t *testing.T) {
	cms1, _ := NewCountMinSketchFromEstimates(0.01, delta)
	cms2, _ := NewCountMinSketchFromEstimates(0.0001, delta)

	cms1.UpdateString("foo", 1)
	cms1.UpdateString("foo", 1)
	cms1.UpdateString("foo", 1)
	cms1.UpdateString("baz", 1)

	cms2.UpdateString("foo", 1)
	cms2.UpdateString("bar", 1)
	cms2.UpdateString("bar", 1)
	cms2.UpdateString("baz", 1)

	err := cms1.Merge(cms2)

	if err == nil {
		t.Errorf("it should error out as cms1 and cms2 are of different sizes")
	}
}

func TestCountMinSketchImportExport(t *testing.T) {
	cms1, _ := NewCountMinSketchFromEstimates(0.001, delta)

	cms1.UpdateString("foo", 1)
	cms1.UpdateString("foo", 1)
	cms1.UpdateString("foo", 1)
	cms1.UpdateString("baz", 1)

	cms2, _ := NewCountMinSketchFromEstimates(0.001, delta)

	cms2.UpdateString("foo", 1)
	cms2.UpdateString("foo", 1)
	cms2.UpdateString("foo", 1)
	cms2.UpdateString("baz", 1)

	sketch1, _ := cms1.Export()
	sketch2, _ := cms2.Export()

	if !reflect.DeepEqual(sketch1, sketch2) {
		t.Errorf("sketch1 and sketch2 should be equal")
	}

	cms3, _ := NewCountMinSketchFromEstimates(0.001, delta)
	cms3.Import(sketch1)

	if !cms1.Equals(cms3) {
		t.Errorf("cms1 and cms3 should be equal")
	}
}

func TestCountMinSketchBinaryReadWrite(t *testing.T) {
	cms1, _ := NewCountMinSketchFromEstimates(0.001, delta)

	cms1.UpdateString("foo", 1)
	cms1.UpdateString("foo", 1)
	cms1.UpdateString("foo", 1)
	cms1.UpdateString("baz", 1)

	var buff bytes.Buffer
	_, err := cms1.WriteTo(&buff)
	if err != nil {
		t.Error("should not error out writing to buffer")
	}

	cms2, _ := NewCountMinSketch(1, 1)
	_, err = cms2.ReadFrom(&buff)
	if err != nil {
		t.Error("should not error out reading from buffer")
	}

	if !cms1.Equals(cms2) {
		t.Error("cms1 and cms2 should be equal")
	}

	e1 := []byte("foo")
	e2 := []byte("bar")
	e3 := []byte("baz")
	c1 := cms2.Count(e1)
	c2 := cms2.Count(e2)
	c3 := cms2.Count(e3)
	if c1 != 3 {
		t.Errorf("count of e1 should be 3, found %d", c1)
	}
	if c2 != 0 {
		t.Errorf("count of e2 should be 0, found %d", c2)
	}
	if c3 != 1 {
		t.Errorf("count of e3 should be 1, found %d", c3)
	}
}

func BenchmarkCMSInsert001X0999(b *testing.B) {
	b.StopTimer()
	cms, _ := NewCountMinSketchFromEstimates(0.001, delta)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cms.Update([]byte(strconv.FormatUint(rand.Uint64(), 10)), 1)
	}
}

func BenchmarkCMSLookup001X0999(b *testing.B) {
	b.StopTimer()
	cms, _ := NewCountMinSketchFromEstimates(0.001, delta)
	for i := 0; i < 1000; i++ {
		cms.Update([]byte(strconv.FormatUint(rand.Uint64(), 10)), 1)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cms.Count([]byte(strconv.FormatUint(rand.Uint64(), 10)))
	}
}

func BenchmarkCMSLookup0001X09999(b *testing.B) {
	b.StopTimer()
	cms, _ := NewCountMinSketchFromEstimates(0.00001, 0.99999)
	for i := 0; i < 100000; i++ {
		cms.Update([]byte(strconv.FormatUint(rand.Uint64(), 10)), 1)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cms.Count([]byte(strconv.FormatUint(rand.Uint64(), 10)))
	}
}

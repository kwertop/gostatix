package gostatix

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func TestCountMinSketchRedisBasic(t *testing.T) {
	initMockRedis()
	cms, _ := NewCountMinSketchRedisFromEstimates(0.001, delta)
	e1 := []byte("foo")
	e2 := []byte("bar")
	e3 := []byte("baz")
	cms.UpdateOnce(e1)
	cms.UpdateOnce(e1)
	cms.UpdateOnce(e2)
	c1, _ := cms.Count(e1)
	c2, _ := cms.Count(e2)
	c3, _ := cms.Count(e3)
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

func TestCountMinSketchRedisMerge(t *testing.T) {
	initMockRedis()
	cms1, _ := NewCountMinSketchRedisFromEstimates(0.001, delta)
	cms2, _ := NewCountMinSketchRedisFromEstimates(0.001, delta)

	cms1.UpdateString("foo", 1)
	cms1.UpdateString("foo", 1)
	cms1.UpdateString("foo", 1)
	cms1.UpdateString("baz", 1)

	cms2.UpdateString("foo", 1)
	cms2.UpdateString("bar", 1)
	cms2.UpdateString("bar", 1)
	cms2.UpdateString("baz", 1)

	cms1.Merge(cms2)

	ok1, _ := cms1.CountString("foo")
	ok2, _ := cms1.CountString("bar")
	ok3, _ := cms1.CountString("baz")
	ok4, _ := cms1.CountString("faz")

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

func TestCountMinSketchRedisMergeError(t *testing.T) {
	initMockRedis()
	cms1, _ := NewCountMinSketchRedisFromEstimates(0.01, delta)
	cms2, _ := NewCountMinSketchRedisFromEstimates(0.0001, delta)

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

func TestCountMinSketchRedisImportExport(t *testing.T) {
	initMockRedis()
	cms1, _ := NewCountMinSketchRedisFromEstimates(0.001, delta)

	cms1.UpdateString("foo", 1)
	cms1.UpdateString("foo", 1)
	cms1.UpdateString("foo", 1)
	cms1.UpdateString("baz", 1)

	cms2, _ := NewCountMinSketchRedisFromEstimates(0.001, delta)

	cms2.UpdateString("foo", 1)
	cms2.UpdateString("foo", 1)
	cms2.UpdateString("foo", 1)
	cms2.UpdateString("baz", 1)

	sketch1, _ := cms1.Export()

	cms3, _ := NewCountMinSketchRedisFromEstimates(0.001, delta)
	cms3.Import(sketch1, true)

	ok, _ := cms1.Equals(cms3)
	if !ok {
		t.Errorf("cms1 and cms3 should be equal")
	}
}

func TestCountMinSketchRedisImportFromKey(t *testing.T) {
	initMockRedis()
	cms1, _ := NewCountMinSketchRedisFromEstimates(0.001, delta)

	cms1.UpdateString("foo", 1)
	cms1.UpdateString("foo", 1)
	cms1.UpdateString("foo", 1)
	cms1.UpdateString("baz", 1)

	key := cms1.MetadataKey()
	cms2, _ := NewCountMinSketchRedisFromKey(key)

	ok, _ := cms1.Equals(cms2)
	if !ok {
		t.Errorf("cms1 and cms3 should be equal")
	}
}

func initMockRedis() {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := ParseRedisURI(redisUri)
	MakeRedisClient(*connOptions)
}

func BenchmarkCMSRedisInsert001X0999(b *testing.B) {
	b.StopTimer()
	connOpts, _ := ParseRedisURI("redis://127.0.0.1:6379")
	MakeRedisClient(*connOpts)
	cms, _ := NewCountMinSketchRedisFromEstimates(0.001, delta)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cms.Update([]byte(strconv.FormatUint(rand.Uint64(), 10)), 1)
	}
}

func BenchmarkCMSRedisLookup001X0999(b *testing.B) {
	b.StopTimer()
	connOpts, _ := ParseRedisURI("redis://127.0.0.1:6379")
	MakeRedisClient(*connOpts)
	cms, _ := NewCountMinSketchRedisFromEstimates(0.001, delta)
	for i := 0; i < 1000; i++ {
		cms.Update([]byte(strconv.FormatUint(rand.Uint64(), 10)), 1)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cms.Count([]byte(strconv.FormatUint(rand.Uint64(), 10)))
	}
}

func BenchmarkCMSRedisLookup0001X09999(b *testing.B) {
	b.StopTimer()
	connOpts, _ := ParseRedisURI("redis://127.0.0.1:6379")
	MakeRedisClient(*connOpts)
	cms, _ := NewCountMinSketchRedisFromEstimates(0.00001, 0.99999)
	for i := 0; i < 100000; i++ {
		cms.Update([]byte(strconv.FormatUint(rand.Uint64(), 10)), 1)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		cms.Count([]byte(strconv.FormatUint(rand.Uint64(), 10)))
	}
}

package count

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/kwertop/gostatix"
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

func initMockRedis() {
	mr, _ := miniredis.Run()
	redisUri := "redis://" + mr.Addr()
	connOptions, _ := gostatix.ParseRedisURI(redisUri)
	gostatix.MakeRedisClient(*connOptions)
}

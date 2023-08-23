package gostatix

import (
	"bytes"
	"testing"
)

func TestBasicBucketMem(t *testing.T) {
	bucket := NewBucketMem(100)
	bucket.Add("foo")
	bucket.Add("bar")
	bucket.Add("baz")
	e := bucket.At(0)
	if e != "foo" {
		t.Errorf("e should be %v", "foo")
	}
	e = bucket.At(2)
	if e != "baz" {
		t.Errorf("e should be %v", "baz")
	}
	i := bucket.NextSlot()
	if i != 3 {
		t.Error("next empty slot should be at 3")
	}
	bucket.Remove("bar")
	e = bucket.At(1)
	if e != "" {
		t.Error("e should be empty string")
	}
	bucket.Set(1, "faz")
	ok := bucket.Lookup("faz")
	if !ok {
		t.Error("faz should be present in the bucket")
	}
	ok = bucket.Lookup("far")
	if ok {
		t.Error("far shouldn't be present in the bucket")
	}
}

func TestBucketFull(t *testing.T) {
	bucket := NewBucketMem(4)
	bucket.Add("foo")
	bucket.Add("bar")
	bucket.Add("baz")
	bucket.Add("faz")
	ok := bucket.Add("far")
	if ok {
		t.Error("far shouldn't be added as bucket is full")
	}
}

func TestBucketLength(t *testing.T) {
	bucket := NewBucketMem(10)
	bucket.Add("foo")
	bucket.Add("bar")
	bucket.Add("baz")
	l := bucket.Length()
	if l != 3 {
		t.Error("bucket length should be 3")
	}
	bucket.Remove("foo")
	l = bucket.Length()
	if l != 2 {
		t.Error("bucket length should be 2")
	}
}

func TestBucketRemove(t *testing.T) {
	bucket := NewBucketMem(3)
	bucket.Add("foo")
	bucket.Add("bar")
	bucket.Add("baz")
	ok1 := bucket.Remove("foo")
	ok2 := bucket.Remove("foo")
	if !ok1 {
		t.Error("foo should be removed as it's present in bucket")
	}
	if ok2 {
		t.Error("can't remove foo as it isn't in the bucket")
	}
}

func TestBucketMemEquals(t *testing.T) {
	b1 := NewBucketMem(10)
	b1.Add("foo")
	b1.Add("bar")
	b1.Add("baz")
	b2 := NewBucketMem(10)
	b2.Add("foo")
	b2.Add("bar")
	b2.Add("baz")
	ok := b1.Equals(b2)
	if !ok {
		t.Error("b1 and b2 should be equal")
	}
	b2.Remove("foo")
	ok = b1.Equals(b2)
	if ok {
		t.Error("b1 and b2 shouldn't be equal here")
	}
}

func TestBucketMemBinaryReadWrite(t *testing.T) {
	b1 := NewBucketMem(10)
	b1.Add("foo")
	b1.Add("bar")
	b1.Add("baz")

	var buff bytes.Buffer
	_, err := b1.WriteTo(&buff)
	if err != nil {
		t.Error("should not error out in writing to buffer")
	}

	b2 := NewBucketMem(0)
	_, err = b2.ReadFrom(&buff)
	if err != nil {
		t.Error("should not error out in reading from buffer")
	}

	ok := b1.Equals(b2)
	if !ok {
		t.Error("b1 and b2 should be equal")
	}

	if ok = b2.Lookup("faz"); ok {
		t.Error("faz should not be present in b2")
	}

	if ok = b2.Lookup("foo"); !ok {
		t.Error("foo should be present in b2")
	}

	if ok = b2.Lookup("bar"); !ok {
		t.Error("bar should be present in b2")
	}

	b2.Remove("foo")
	ok = b1.Equals(b2)
	if ok {
		t.Error("b1 and b2 shouldn't be equal here")
	}
}

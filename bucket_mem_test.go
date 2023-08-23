package gostatix

import (
	"bytes"
	"testing"
)

func TestBasicBucketMem(t *testing.T) {
	bucket := newBucketMem(100)
	bucket.add("foo")
	bucket.add("bar")
	bucket.add("baz")
	e := bucket.at(0)
	if e != "foo" {
		t.Errorf("e should be %v", "foo")
	}
	e = bucket.at(2)
	if e != "baz" {
		t.Errorf("e should be %v", "baz")
	}
	i := bucket.nextSlot()
	if i != 3 {
		t.Error("next empty slot should be at 3")
	}
	bucket.remove("bar")
	e = bucket.at(1)
	if e != "" {
		t.Error("e should be empty string")
	}
	bucket.set(1, "faz")
	ok := bucket.lookup("faz")
	if !ok {
		t.Error("faz should be present in the bucket")
	}
	ok = bucket.lookup("far")
	if ok {
		t.Error("far shouldn't be present in the bucket")
	}
}

func TestBucketFull(t *testing.T) {
	bucket := newBucketMem(4)
	bucket.add("foo")
	bucket.add("bar")
	bucket.add("baz")
	bucket.add("faz")
	ok := bucket.add("far")
	if ok {
		t.Error("far shouldn't be added as bucket is full")
	}
}

func TestBucketLength(t *testing.T) {
	bucket := newBucketMem(10)
	bucket.add("foo")
	bucket.add("bar")
	bucket.add("baz")
	l := bucket.getLength()
	if l != 3 {
		t.Error("bucket length should be 3")
	}
	bucket.remove("foo")
	l = bucket.getLength()
	if l != 2 {
		t.Error("bucket length should be 2")
	}
}

func TestBucketRemove(t *testing.T) {
	bucket := newBucketMem(3)
	bucket.add("foo")
	bucket.add("bar")
	bucket.add("baz")
	ok1 := bucket.remove("foo")
	ok2 := bucket.remove("foo")
	if !ok1 {
		t.Error("foo should be removed as it's present in bucket")
	}
	if ok2 {
		t.Error("can't remove foo as it isn't in the bucket")
	}
}

func TestBucketMemEquals(t *testing.T) {
	b1 := newBucketMem(10)
	b1.add("foo")
	b1.add("bar")
	b1.add("baz")
	b2 := newBucketMem(10)
	b2.add("foo")
	b2.add("bar")
	b2.add("baz")
	ok := b1.equals(b2)
	if !ok {
		t.Error("b1 and b2 should be equal")
	}
	b2.remove("foo")
	ok = b1.equals(b2)
	if ok {
		t.Error("b1 and b2 shouldn't be equal here")
	}
}

func TestBucketMemBinaryReadWrite(t *testing.T) {
	b1 := newBucketMem(10)
	b1.add("foo")
	b1.add("bar")
	b1.add("baz")

	var buff bytes.Buffer
	_, err := b1.writeTo(&buff)
	if err != nil {
		t.Error("should not error out in writing to buffer")
	}

	b2 := newBucketMem(0)
	_, err = b2.readFrom(&buff)
	if err != nil {
		t.Error("should not error out in reading from buffer")
	}

	ok := b1.equals(b2)
	if !ok {
		t.Error("b1 and b2 should be equal")
	}

	if ok = b2.lookup("faz"); ok {
		t.Error("faz should not be present in b2")
	}

	if ok = b2.lookup("foo"); !ok {
		t.Error("foo should be present in b2")
	}

	if ok = b2.lookup("bar"); !ok {
		t.Error("bar should be present in b2")
	}

	b2.remove("foo")
	ok = b1.equals(b2)
	if ok {
		t.Error("b1 and b2 shouldn't be equal here")
	}
}

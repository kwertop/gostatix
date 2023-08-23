GoStatix ![Tests](https://github.com/kwertop/gostatix/actions/workflows/run_tests.yml/badge.svg)
==========

Thread-safe and persistent Golang implementations of probabilistic data structures: Bloom Filter, Cuckoo Filter, HyperLogLog, Count-Min Sketch and Top-K

# About

This package provides two implementations of the data structures _(a)_ In-memory _(b)_ Redis backed. While keeping data in-memory has the advantages of being higly performant and yielding high throughput, it doesn't serve use cases of portability or communication of the underlying data so well. Take, for example, a setup where two applications are running to fulfill a goal. One of them writes the data and the other reads from it taking decisions based upon that. In this case, there should be a mechanism to share the underlying data structure for the two applications to do their tasks. With some trade-off to performance, this package solves that problem using Redis as the intermediate storage layer to achieve the same.

# Quick Start

## Install

## Bloom Filters

A Bloom filter is a space-efficient probabilistic data structure that is used to test whether an element is a member of a set. It provides a way to check for the presence of an element in a set without actually storing the entire set. Bloom filters are particularly useful in scenarios where memory is limited or when the exact membership information is not critical.

**Refer**: https://web.stanford.edu/~balaji/papers/bloom.pdf

### In-memory

```go
package main

import (
	"fmt"

	"github.com/kwertop/gostatix"
)

func main() {

	// create a new in-memory bloom filter with 1000000 items and a false positive rate of 0.0001
	filter, _ := gostatix.NewMemBloomFilterWithParameters(1000000, 0.001)

	e1 := []byte("cat")
	e2 := []byte("dog")

	// insert a few elements - "cat" and "dog"
	filter.Insert(e1).Insert(e2)

	// do a lookup for an element
	ok := filter.Lookup(e1)
	fmt.Printf("found cat, %v\n", ok) // found cat, true

	ok = filter.Lookup([]byte("elephant"))
	fmt.Printf("found elephant, %v\n", ok) // found elephant, false
}

```

### Redis

```go
package main

import (
	"fmt"

	"github.com/kwertop/gostatix"
)

func main() {
    
    // parse redis uri
	redisConnOpt, _ := gostatix.ParseRedisURI("redis://127.0.0.1:6379")

    // create redis connection
	gostatix.MakeRedisClient(*redisConnOpt)

    // create a new redis bloom filter with 1000000 items and a false positive rate of 0.0001
	bloomRedis, _ := gostatix.NewRedisBloomFilterWithParameters(1000000, 0.001)

    e1 := []byte("cat")
	e2 := []byte("dog")

    // insert a few elements - "cat" and "dog"
	bloomRedis.Insert(e1).Insert(e2)

    // do a lookup for an element
	ok := bloomRedis.Lookup(e1)
	fmt.Printf("found cat, %v\n", ok) // found cat, true

	ok = filter.Lookup([]byte("elephant"))
	fmt.Printf("found elephant, %v\n", ok) // found elephant, false
}

```
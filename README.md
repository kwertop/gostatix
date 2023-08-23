GoStatix ![example workflow](https://github.com/kwertop/gostatix/actions/workflows/run_tests.yml/badge.svg)
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

    "github.com/kwertop/filters"
)

func main() {

    // create a new in-memory bloom filter with 1000000 items and a false positive rate of 0.0001
    filter := filters.NewMemBloomFilterWithParameters(1000000, 0.0001)

    
    filter.Insert([]byte("cat")).Insert([]byte("dog"))

    // isCatPresent is boolean value set to true
    // since "cat" is present in the filter
    isCatPresent := filter.Lookup([]byte("cat"))

    // isMicePresent is boolean value set to false
    // since "mice" is not present in the filter
    isMicePresent := filter.Lookup([]byte("mice"))

}
```

### Redis

```go
package main

import (
    "fmt"

    "github.com/kwertop/filters"
)

func main() {

    // create a new redis-backed bloom filter with 1000000 items and a false positive rate of 0.0001
    filter := filters.NewRedisBloomFilterWithParameters(1000000, 0.0001)

    
    filter.Insert([]byte("cat")).Insert([]byte("dog"))

    // isCatPresent is boolean value set to true
    // since "cat" is present in the filter
    isCatPresent := filter.Lookup([]byte("cat"))

    // isMicePresent is boolean value set to false
    // since "mice" is not present in the filter
    isMicePresent := filter.Lookup([]byte("mice"))

}
```
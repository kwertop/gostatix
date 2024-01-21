GoStatix ![Tests](https://github.com/kwertop/gostatix/actions/workflows/run_tests.yml/badge.svg)
[![Go Reference](https://pkg.go.dev/badge/github.com/kwerrtop/gostatix.svg)](https://pkg.go.dev/github.com/kwertop/gostatix)
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

## Cuckoo Filters

A Cuckoo filter is a data structure used for approximate set membership queries, similar to a Bloom filter. It is designed to provide a compromise between memory efficiency, fast membership queries, and the ability to delete elements from the filter. Unlike a Bloom filter, a Cuckoo filter allows for efficient removal of elements while maintaining relatively low false positive rates.
Refer: https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf

### In-memory

```go
package main

import (
	"fmt"

	"github.com/kwertop/gostatix"
)

func main() {
	// create a new in-memory cuckoo filter with 1000000 items and a false positive rate of 0.0001
    // see doc for more details
	filter := gostatix.NewCuckooFilterWithErrorRate(100000, 4, 100, 0.001)

	e1 := []byte("cat")
	e2 := []byte("dog")

	// insert a few elements - "cat" and "dog"
	filter.Insert(e1, false)
    filter.Insert(e2, false)

	// do a lookup for an element
	ok := filter.Lookup(e1)
	fmt.Printf("found cat, %v\n", ok) // found cat, true

	ok = filter.Lookup([]byte("elephant"))
	fmt.Printf("found elephant, %v\n", ok) // found elephant, false

	ok = filter.Lookup(e2)
	fmt.Printf("found dog, %v\n", ok) // found dog, true

	//remove an element
	_ = filter.Remove(e2)

	ok = filter.Lookup(e2)
	fmt.Printf("found dog, %v\n", ok) // found dog, false
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

    // create a new redis backed cuckoo filter with 1000000 items and a false positive rate of 0.0001
    // see doc for more details
    filter, _ := gostatix.NewCuckooFilterRedisWithErrorRate(1000000, 4, 100, 0.001)

    // insert a few elements - "cat" and "dog"
    filter.Insert(e1, false)
    filter.Insert(e2, false)

    // do a lookup for an element
    ok, _ = filter.Lookup(e1)
    fmt.Printf("found cat, %v\n", ok) // found cat, true

    ok, _ = filter.Lookup([]byte("elephant"))
    fmt.Printf("found elephant, %v\n", ok) // found elephant, false

    ok, _ = filter.Lookup(e2)
    fmt.Printf("found dog, %v\n", ok) // found dog, true

    //remove an element
    _, _ = filter.Remove(e2)

    ok, _ = filter.Lookup(e2)
    fmt.Printf("found dog, %v\n", ok) // found dog, false
}
```

## Count-Min Sketch

A probabilistic data structure used to estimate the frequency of items in a data stream.
Refer: http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf

### In-memory

```go
package main

import (
	"fmt"

	"github.com/kwertop/gostatix"
)

func main() {
	// create a new in-memory count-min sketch with error rate of 0.0001 and delta of 0.9999
	sketch, _ := gostatix.NewCountMinSketchFromEstimates(0.0001, 0.9999)

	e1 := []byte("cat")
	e2 := []byte("dog")

	// insert counts of few elements - "cat" and "dog"
	sketch.Update(e1, 2)
	sketch.Update(e2, 3)
	sketch.Update(e1, 1)

	// do a lookup for an count for any element
	count := sketch.Count(e1)
	fmt.Printf("found %v cats\n", count) // found 3 cats
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
    // create redis connection
    gostatix.MakeRedisClient(*redisConnOpt)

    // create a new redis backed count-min sketch with error rate of 0.0001 and delta of 0.9999
    sketch, _ := gostatix.NewCountMinSketchRedisFromEstimates(0.001, 0.999)

    e1 := []byte("cat")
    e2 := []byte("dog")

    // insert counts of few elements - "cat" and "dog"
    err := sketch.Update(e1, 2)
    if err != nil {
        fmt.Printf("error: %v\n", err)
    }
    sketch.Update(e2, 3)
    sketch.Update(e1, 1)

    // do a lookup for an count for any element
    count, _ = sketch.Count(e1)
    fmt.Printf("found %v cats\n", count) // found 3 cats
}

```

## HyperLogLog

A probabilistic data structure used for estimating the cardinality (number of unique elements) of in a very large dataset.
Refer: https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf

### In-memory

```go
package main

import (
	"fmt"

	"github.com/kwertop/gostatix"
)

func main() {
	// create a new in-memory hyperloglog with 4 registers
	hll, _ := gostatix.NewHyperLogLog(4)

	e1 := []byte("cat")
	e2 := []byte("dog")

	// insert counts of few elements
	hll.Update(e1)
	hll.Update(e2)
	hll.Update(e1)

	// do a lookup for count of distinct elements
	distinct := hll.Count(true, true)
	fmt.Printf("found %v distinct elements\n", distinct) // found 2 distinct elements
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

    // create a new redis backed hyperloglog with 4 registers
    hll, _ := gostatix.NewHyperLogLogRedis(4)

    e1 := []byte("cat")
    e2 := []byte("dog")

    // insert counts of few elements
    hll.Update(e1)
    hll.Update(e2)
    hll.Update(e1)

    // do a lookup for count of distinct elements
    distinct, _ := hll.Count(true, true)
    fmt.Printf("found %v distinct elements\n", distinct) // found 2 distinct elements
}

```

## Top-K

It's a data structure designed to efficiently retrieve the "top-K" or "largest-K" elements from a dataset based on a certain criterion, such as frequency, value, or score.

### In-memory

```go
package main

import (
	"fmt"

	"github.com/kwertop/gostatix"
)

func main() {
	// create a new in-memory top-k with k=2, error rate of 0.001 and delta of 0.999
	t := gostatix.NewTopK(2, 0.001, 0.999)

	e1 := []byte("cat")
	e2 := []byte("dog")
	e3 := []byte("lion")
	e4 := []byte("tiger")

	// insert counts of few elements
	t.Insert(e1, 3)
	t.Insert(e2, 2)
	t.Insert(e1, 1)
	t.Insert(e3, 3)
	t.Insert(e4, 1)

	// do a lookup for top-k (2) elements
	values := t.Values()
	fmt.Printf("%v\n", values) // [{cat 4} {lion 3}]
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

    // create a new redis backed top-k with k=2, error rate of 0.001 and delta of 0.999
    t1 := gostatix.NewTopKRedis(2, 0.001, 0.999)

    e1 := []byte("cat")
    e2 := []byte("dog")
    e3 := []byte("lion")
    e4 := []byte("tiger")

    // insert counts of few elements
    t1.Insert(e1, 3)
    t1.Insert(e2, 2)
    t1.Insert(e1, 1)
    t1.Insert(e3, 3)
    t1.Insert(e4, 1)

    // do a lookup for top-k (2) elements
    values1, _ := t1.Values()
    fmt.Printf("%v\n", values1) // [{cat 4} {lion 3}]
}
```
/*
Package filters provides data structures and methods for creating probabilistic filters.
This package provides implementations of two of the most widely used filters,
Bloom Filter and Cuckoo Filter.

A Bloom filter is a space-efficient probabilistic data structure that is used to test
whether an element is a member of a set. It provides a way to check for the presence of
an element in a set without actually storing the entire set. Bloom filters are particularly
useful in scenarios where memory is limited or when the exact membership information is not critical.
Refer: https://web.stanford.edu/~balaji/papers/bloom.pdf

A Cuckoo filter is a data structure used for approximate set membership queries, similar to a
Bloom filter. It is designed to provide a compromise between memory efficiency, fast membership queries,
and the ability to delete elements from the filter. Unlike a Bloom filter, a Cuckoo filter allows for
efficient removal of elements while maintaining relatively low false positive rates.
Refer: https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf
*/
package gostatix

type BaseFilter[T any] interface {
	Insert(element T) (bool, error)
	Lookup(element T) (bool, error)
}

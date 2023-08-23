/*
Package buckets implements buckets - a container of fixed number of entries
used in cuckoo filters.
*/
package gostatix

type BaseBucket interface {
	Size() uint64
}

type AbstractBucket struct {
	BaseBucket
	size uint64
}

// Size returns the number of entries in the bucket
func (bucket *AbstractBucket) Size() uint64 {
	return bucket.size
}

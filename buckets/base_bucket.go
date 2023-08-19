package buckets

type BaseBucket interface {
	Size() uint64
}

type AbstractBucket struct {
	BaseBucket
	size uint64
}

func (bucket *AbstractBucket) Size() uint64 {
	return bucket.size
}

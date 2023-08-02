package buckets

type BaseBucket interface {
	Size() uint64
	Length() uint64
	IsFree() bool
}

type AbstractBucket struct {
	BaseBucket
	size   uint64
	length uint64
}

func (bucket *AbstractBucket) Size() uint64 {
	return bucket.size
}

func (bucket *AbstractBucket) Length() uint64 {
	return bucket.length
}

func (bucket *AbstractBucket) IsFree() bool {
	return bucket.length < bucket.size
}

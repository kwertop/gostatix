package buckets

type BucketMem struct {
	elements []string
	BaseBucket
}

func NewBucketMem(size uint64) *BucketMem {
	return &BucketMem{make([]string, size), BaseBucket{size, 0}}
}

func (bucket BucketMem) Size() uint64 {
	return bucket.size
}

func (bucket BucketMem) Length() uint64 {
	return bucket.length
}

func (bucket BucketMem) IsFree() bool {
	return bucket.length < bucket.size
}

func (bucket BucketMem) NextSlot() int64 {
	return bucket.indexOf("")
}

func (bucket BucketMem) At(index uint64) string {
	return bucket.elements[index]
}

func (bucket *BucketMem) Add(element string) bool {
	if element == "" || !bucket.IsFree() {
		return false
	}
	bucket.Set(uint64(bucket.NextSlot()), element)
	bucket.length++
	return true
}

func (bucket BucketMem) Remove(element string) bool {
	index := bucket.indexOf(element)
	if index <= -1 {
		return false
	}
	bucket.UnSet(uint64(index))
	return true
}

func (bucket BucketMem) Lookup(element string) bool {
	return bucket.indexOf(element) > -1
}

func (bucket BucketMem) Set(index uint64, element string) {
	bucket.elements[index] = element
}

func (bucket *BucketMem) UnSet(index uint64) {
	bucket.elements[index] = ""
	bucket.length--
}

func (bucket *BucketMem) Swap(index uint64, element string) string {
	temp := bucket.elements[index]
	bucket.elements[index] = element
	return temp
}

func (bucket BucketMem) Equals(otherBucket BucketMem) bool {
	if bucket.size != otherBucket.size || bucket.length != otherBucket.length {
		return false
	}
	for index, val := range bucket.elements {
		if otherBucket.elements[index] != val {
			return false
		}
	}
	return true
}

func (bucket BucketMem) indexOf(element string) int64 {
	for index, val := range bucket.elements {
		if val == element {
			return int64(index)
		}
	}
	return int64(-1)
}

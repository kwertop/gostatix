package filters

type CuckooFilterRedis struct {
	buckets []string
	*AbstractCuckooFilter
}

// func NewCuckooFilterRedis(size, bucketSize, fingerPrintLength uint64) *CuckooFilterRedis {
// 	baseFilter := MakeAbstractCuckooFilter(size, bucketSize, fingerPrintLength, 0, 500)
// }

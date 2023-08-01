package filters

type CuckooFilterRedis struct {
	size              uint64
	bucketSize        uint64
	fingerPrintLength uint64
	length            uint64
	retries           uint64
}

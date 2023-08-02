package filters

type CuckooFilterRedis struct {
	buckets []string
	*AbstractCuckooFilter
}

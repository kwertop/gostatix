package gostatix

import (
	"math"
)

func CalculateFilterSize(length uint, errorRate float64) uint {
	return uint(math.Ceil(-((float64(length) * math.Log(errorRate)) / math.Pow(math.Log(2), 2))))
}

func CalculateNumHashes(size, length uint) uint {
	return uint(math.Ceil(float64((size / length)) * math.Log(2)))
}

func CalculateFingerPrintLength(size uint64, errorRate float64) uint64 {
	v := math.Ceil(math.Log2(1/errorRate) + math.Log2(float64(2*size)))
	return uint64(math.Ceil(v / 8)) //gostatix uses 64 bit hash for cuckoo filter
}

type BitSetType int

const (
	RedisBitSet BitSetType = iota
	InMemoryBitSet
)

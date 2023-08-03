package gostatix

import (
	"math"
	"math/rand"
	"time"
	"unsafe"
)

var src = rand.NewSource(time.Now().UnixNano())

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
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

func GenerateRandomString(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}

type BitSetType int

const (
	RedisBitSet BitSetType = iota
	InMemoryBitSet
)

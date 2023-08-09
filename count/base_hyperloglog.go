package count

import (
	"fmt"
	"math"
	"strconv"

	"github.com/kwertop/gostatix/hash"
)

type BaseHyperLogLog interface {
	NumRegisters() uint64
	Accuracy() float64
	Count(withCorrection bool, withRoundingOff bool) uint64
	Update(data []byte)
	Equals(g *HyperLogLog) bool
}

type AbstractHyperLogLog struct {
	BaseHyperLogLog
	numRegisters    uint64
	numBytesPerHash uint64
	correctionBias  float64
}

func MakeAbstractHyperLogLog(numRegisters uint64) (*AbstractHyperLogLog, error) {
	if numRegisters == 0 {
		panic("gostatix: hyperloglog number of registers can't be zero")
	}
	if numRegisters&(numRegisters-1) != 0 {
		return nil, fmt.Errorf("gostatix: hyperloglog number of registers %d not a power of two", numRegisters)
	}
	h := &AbstractHyperLogLog{}
	h.numRegisters = numRegisters
	h.numBytesPerHash = uint64(math.Log2(float64(numRegisters)))
	h.correctionBias = getAlpha(uint(numRegisters))
	return h, nil
}

func (h *AbstractHyperLogLog) NumRegisters() uint64 {
	return h.numRegisters
}

func (h *AbstractHyperLogLog) Accuracy() float64 {
	return 1.04 / math.Sqrt(float64(h.numRegisters))
}

func getAlpha(m uint) (result float64) {
	switch m {
	case 16:
		result = 0.673
	case 32:
		result = 0.697
	case 64:
		result = 0.709
	default:
		result = 0.7213 / (1.0 + 1.079/float64(m))
	}
	return result
}

func (h *AbstractHyperLogLog) getRegisterIndexAndCount(data []byte) (int, int) {
	hash, _ := hash.Sum128(data)
	hashString := strconv.FormatUint(hash, 10)
	registerIndex, _ := strconv.Atoi(hashString[0:(h.numBytesPerHash - 1)])
	registerIndex++
	secondPart := hashString[h.numBytesPerHash:]
	count := 0
	for secondPart[count] != '1' && count < len(secondPart)-1 {
		count++
	}
	return registerIndex, count
}

func (h *AbstractHyperLogLog) getEstimation(harmonicMean float64, withCorrection, withRoundingOff bool) uint64 {
	estimation := (h.correctionBias * math.Pow(float64(h.numRegisters), 2)) / harmonicMean
	towPow32 := math.Pow(2, 32)
	if estimation > towPow32/30 && withCorrection {
		estimation = -towPow32 * math.Log(1-estimation/towPow32)
	}
	if withRoundingOff {
		estimation = math.Round(estimation)
	}
	return uint64(estimation)
}

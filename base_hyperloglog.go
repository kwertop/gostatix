/*
Implements probabilistic data structure hyperloglog used in estimating unique entries in a
large dataset.

Hyperloglog: A probabilistic data structure used for estimating the cardinality
(number of unique elements) of in a very large dataset.
Refer: https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf

The package implements both in-mem and Redis backed solutions for the data structures. The
in-memory data structures are thread-safe.
*/
package gostatix

import (
	"fmt"
	"math"
	"math/bits"

	"github.com/dgryski/go-metro"
)

// Interface for Hyperloglog
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

type hyperLogLogJSON struct {
	NumRegisters    uint64  `json:"nr"`
	NumBytesPerHash uint64  `json:"nbp"`
	CorrectionBias  float64 `json:"c"`
	Registers       []uint8 `json:"r"`
	Key             string  `json:"k"`
}

func makeAbstractHyperLogLog(numRegisters uint64) (*AbstractHyperLogLog, error) {
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

// NumRegisters returns the number of registers in the hyperloglog
func (h *AbstractHyperLogLog) NumRegisters() uint64 {
	return h.numRegisters
}

// Accuracy returns the accuracy of the hyperloglog
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

func (h *AbstractHyperLogLog) getRegisterIndexAndCount(data []byte) (uint64, uint64) {
	hash, _ := metro.Hash128(data, 1373)
	k := 32 - h.numBytesPerHash
	registerIndex := 1 + bits.LeadingZeros64(hash<<h.numBytesPerHash)
	count := hash >> uint(k)
	return uint64(registerIndex), count
}

func (h *AbstractHyperLogLog) getEstimation(harmonicMean float64, withCorrection, withRoundingOff bool) uint64 {
	estimation := (h.correctionBias * math.Pow(float64(h.numRegisters), 2)) / harmonicMean
	twoPow32 := math.Pow(2, 32)
	if estimation > twoPow32/30 && withCorrection {
		estimation = -twoPow32 * math.Log(1-estimation/twoPow32)
	}
	if withRoundingOff {
		estimation = math.Round(estimation)
	}
	return uint64(estimation)
}

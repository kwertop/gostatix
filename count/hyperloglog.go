package count

import (
	"fmt"
	"math"
	"strconv"

	"github.com/kwertop/gostatix"
	"github.com/kwertop/gostatix/hash"
)

type HyperLogLog struct {
	numRegisters    uint64
	numBytesPerHash uint64
	correctionBias  float64
	registers       []uint8
}

func NewHyperLogLog(numRegisters uint64) (*HyperLogLog, error) {
	if numRegisters == 0 {
		panic("gostatix: hyperloglog number of registers can't be zero")
	}
	if numRegisters&(numRegisters-1) != 0 {
		return nil, fmt.Errorf("gostatix: hyperloglog number of registers %d not a power of two", numRegisters)
	}
	h := &HyperLogLog{}
	h.numRegisters = numRegisters
	h.numBytesPerHash = uint64(math.Log2(float64(numRegisters)))
	h.correctionBias = getAlpha(uint(numRegisters))
	h.registers = make([]uint8, numRegisters)
	return h, nil
}

func (h *HyperLogLog) Reset() {
	for i := range h.registers {
		h.registers[i] = 0
	}
}

func (h *HyperLogLog) NumRegisters() uint64 {
	return h.numRegisters
}

func (h *HyperLogLog) Update(data []byte) {
	hash, _ := hash.Sum128(data)
	hashString := strconv.FormatUint(hash, 10)
	registerIndex, _ := strconv.Atoi(hashString[0:(h.numBytesPerHash - 1)])
	registerIndex++
	secondPart := hashString[h.numBytesPerHash:]
	leftPosition := 0
	for secondPart[leftPosition] != '1' && leftPosition < len(secondPart)-1 {
		leftPosition++
	}
	h.registers[registerIndex] = uint8(gostatix.Max(uint(h.registers[registerIndex]), uint(leftPosition)))
}

func (h *HyperLogLog) Count(withCorrection bool, withRoundingOff bool) uint64 {
	harmonicMean := 0.0
	for i := range h.registers {
		harmonicMean += math.Pow(2, -float64(h.registers[i]))
	}
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

func (h *HyperLogLog) Accuracy() float64 {
	return 1.04 / math.Sqrt(float64(h.numRegisters))
}

func (h *HyperLogLog) Merge(g *HyperLogLog) error {
	if h.numRegisters != g.numRegisters {
		return fmt.Errorf("gostatix: number of registers %d, %d don't match", h.numRegisters, g.numRegisters)
	}
	for i := range g.registers {
		h.registers[i] = uint8(gostatix.Max(uint(h.registers[i]), uint(g.registers[i])))
	}
	return nil
}

func (h *HyperLogLog) Equals(g *HyperLogLog) bool {
	if h.numRegisters != g.numRegisters {
		return false
	}
	for i := 0; i < int(h.numRegisters)-1; i++ {
		if h.registers[i] != g.registers[i] {
			return false
		}
	}
	return true
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

package count

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/kwertop/gostatix"
)

type HyperLogLog struct {
	AbstractHyperLogLog
	registers []uint8
}

func NewHyperLogLog(numRegisters uint64) (*HyperLogLog, error) {
	registers := make([]uint8, numRegisters)
	abstractLog, err := MakeAbstractHyperLogLog(numRegisters)
	if err != nil {
		return nil, err
	}
	h := &HyperLogLog{*abstractLog, registers}
	return h, nil
}

func (h *HyperLogLog) Reset() {
	for i := range h.registers {
		h.registers[i] = 0
	}
}

func (h *HyperLogLog) Update(data []byte) {
	registerIndex, count := h.getRegisterIndexAndCount(data)
	h.registers[registerIndex] = uint8(gostatix.Max(uint(h.registers[registerIndex]), uint(count)))
}

func (h *HyperLogLog) Count(withCorrection, withRoundingOff bool) uint64 {
	harmonicMean := 0.0
	for i := range h.registers {
		harmonicMean += math.Pow(2, -float64(h.registers[i]))
	}
	return h.getEstimation(harmonicMean, withCorrection, withRoundingOff)
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

func (h *HyperLogLog) Export() ([]byte, error) {
	return json.Marshal(hyperLogLogJSON{h.numRegisters, h.numBytesPerHash, h.correctionBias, h.registers, ""})
}

func (h *HyperLogLog) Import(data []byte) error {
	var g hyperLogLogJSON
	err := json.Unmarshal(data, &g)
	if err != nil {
		return err
	}
	h.numRegisters = g.NumRegisters
	h.numBytesPerHash = g.NumBytesPerHash
	h.correctionBias = g.CorrectionBias
	h.registers = g.Registers
	return nil
}

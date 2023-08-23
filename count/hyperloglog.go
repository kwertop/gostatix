/*
Package count implements various probabilistic data structures used in counting.

 1. Count-Min Sketch: A probabilistic data structure used to estimate the frequency
    of items in a data stream. Refer: http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf
 2. Hyperloglog: A probabilistic data structure used for estimating the cardinality
    (number of unique elements) of in a very large dataset.
    Refer: https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
 3. Top-K: A data structure designed to efficiently retrieve the "top-K" or "largest-K"
    elements from a dataset based on a certain criterion, such as frequency, value, or score

The package implements both in-mem and Redis backed solutions for the data structures. The
in-memory data structures are thread-safe.
*/
package count

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/kwertop/gostatix"
)

// HyperLogLog struct. This is an in-memory implementation of HyperLogLog.
// It's mainly governed by a 1-d slice _registers_ which holds the count of hashed items
// at different hashed locations
// _numRegisters_ is used to specify the size of the _registers_ slice
// _lock_ is used to synchronize concurrent read/writes
type HyperLogLog struct {
	AbstractHyperLogLog
	registers []uint8
	lock      sync.RWMutex
}

// NewHyperLogLog creates new HyperLogLog with the specified _numRegisters_
func NewHyperLogLog(numRegisters uint64) (*HyperLogLog, error) {
	registers := make([]uint8, numRegisters)
	abstractLog, err := MakeAbstractHyperLogLog(numRegisters)
	if err != nil {
		return nil, err
	}
	h := &HyperLogLog{AbstractHyperLogLog: *abstractLog, registers: registers}
	return h, nil
}

// Reset sets all values in the _registers_ slice to zero
func (h *HyperLogLog) Reset() {
	for i := range h.registers {
		h.registers[i] = 0
	}
}

// Update sets the count of the passed _data_ (byte slice) to the hashed location
// in the _registers_ slice
func (h *HyperLogLog) Update(data []byte) {
	h.lock.Lock()
	defer h.lock.Unlock()

	registerIndex, count := h.getRegisterIndexAndCount(data)
	h.registers[registerIndex] = uint8(gostatix.Max(uint(h.registers[registerIndex]), uint(count)))
}

// Count returns the number of distinct elements so far
// _withCorrection_ is used to specify if correction is to be done for large registers
// _withRoundingOff_ is used to specify if rounding off is required for estimation
func (h *HyperLogLog) Count(withCorrection, withRoundingOff bool) uint64 {
	h.lock.Lock()
	defer h.lock.Unlock()

	harmonicMean := 0.0
	for i := range h.registers {
		harmonicMean += math.Pow(2, -float64(h.registers[i]))
	}
	return h.getEstimation(harmonicMean, withCorrection, withRoundingOff)
}

// Merge merges two Hyperloglog data structures
func (h *HyperLogLog) Merge(g *HyperLogLog) error {
	if h.numRegisters != g.numRegisters {
		return fmt.Errorf("gostatix: number of registers %d, %d don't match", h.numRegisters, g.numRegisters)
	}
	for i := range g.registers {
		h.registers[i] = uint8(gostatix.Max(uint(h.registers[i]), uint(g.registers[i])))
	}
	return nil
}

// Equals checks if two Hyperloglog data structures are equal
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

// Export JSON marshals the HyperLogLog and returns a byte slice containing the data
func (h *HyperLogLog) Export() ([]byte, error) {
	return json.Marshal(hyperLogLogJSON{h.numRegisters, h.numBytesPerHash, h.correctionBias, h.registers, ""})
}

// Import JSON unmarshals the _data_ into the HyperLogLog
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

// WriteTo writes the HyperLogLog onto the specified _stream_ and returns the
// number of bytes written.
// It can be used to write to disk (using a file stream) or to network.
func (h *HyperLogLog) WriteTo(stream io.Writer) (int64, error) {
	err := binary.Write(stream, binary.BigEndian, h.numRegisters)
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, h.numBytesPerHash)
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, h.correctionBias)
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, h.registers)
	if err != nil {
		return 0, err
	}
	return int64((h.numRegisters + 3) * uint64(binary.Size(uint64(0)))), nil
}

// ReadFrom reads the BloomFilter from the specified _stream_ and returns the
// number of bytes read.
// It can be used to read from disk (using a file stream) or from network.
func (h *HyperLogLog) ReadFrom(stream io.Reader) (int64, error) {
	var numRegisters, numBytesPerHash uint64
	err := binary.Read(stream, binary.BigEndian, &numRegisters)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &numBytesPerHash)
	if err != nil {
		return 0, err
	}
	var correctionBias float64
	err = binary.Read(stream, binary.BigEndian, &correctionBias)
	if err != nil {
		return 0, err
	}
	h.numRegisters = numRegisters
	h.numBytesPerHash = numBytesPerHash
	h.correctionBias = correctionBias
	registers := make([]uint8, numRegisters)
	err = binary.Read(stream, binary.BigEndian, &registers)
	if err != nil {
		return 0, err
	}
	h.registers = registers
	return int64((h.numRegisters + 3) * uint64(binary.Size(uint64(0)))), nil
}

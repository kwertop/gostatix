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
package gostatix

import (
	"github.com/dgryski/go-metro"
)

// Interface for Count-Min Sketch
type BaseCountMinSketch interface {
	GetRows() uint
	GetColumns() uint
	Update(data []byte, count uint64)
	UpdateString(data string, count uint64)
	Count(data []byte) uint64
	CountString(data string) uint64
	UpdateOnce(data []byte)
}

type AbstractCountMinSketch struct {
	BaseCountMinSketch
	rows    uint
	columns uint
	allSum  uint64
}

func makeAbstractCountMinSketch(rows, columns uint, allSum uint64) *AbstractCountMinSketch {
	cms := &AbstractCountMinSketch{}
	cms.rows = rows
	cms.columns = columns
	cms.allSum = allSum
	return cms
}

// GetRows returns the number of rows in the underlying matrix of the Count-Min Sketch
func (cms *AbstractCountMinSketch) GetRows() uint {
	return cms.rows
}

// GetColumns returns the number of columns in the underlying matrix of the Count-Min Sketch
func (cms *AbstractCountMinSketch) GetColumns() uint {
	return cms.columns
}

func (cms *AbstractCountMinSketch) getPositions(data []byte) []uint {
	positions := make([]uint, cms.rows)
	hash1, hash2 := metro.Hash128(data, 1373)
	for c := range positions {
		positions[c] = uint((hash1 + uint64(c)*hash2) % uint64(cms.columns))
	}
	return positions
}

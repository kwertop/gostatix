package count

import (
	"github.com/dgryski/go-metro"
)

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

func MakeAbstractCountMinSketch(rows, columns uint, allSum uint64) *AbstractCountMinSketch {
	cms := &AbstractCountMinSketch{}
	cms.rows = rows
	cms.columns = columns
	cms.allSum = allSum
	return cms
}

func (cms *AbstractCountMinSketch) GetRows() uint {
	return cms.rows
}

func (cms *AbstractCountMinSketch) GetColumns() uint {
	return cms.columns
}

func (cms AbstractCountMinSketch) getPositions(data []byte) []uint {
	positions := make([]uint, cms.rows)
	hash1, hash2 := metro.Hash128(data, 1373)
	for c := range positions {
		positions[c] = uint((hash1 + uint64(c)*hash2) % uint64(cms.columns))
	}
	return positions
}

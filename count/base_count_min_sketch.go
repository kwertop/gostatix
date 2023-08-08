package count

import "github.com/kwertop/gostatix/hash"

type BaseCountMinSketch interface {
	GetRows() uint
	GetColumns() uint
	Update(data []byte, count uint64)
	UpdateString(data string, count uint64)
	Count(data []byte) uint64
	CountString(data string) uint64
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
	positions := make([]uint, cms.columns)
	hash1, hash2 := hash.Sum128(data)
	for c := range positions {
		positions[c] = uint((hash1 + uint64(c)*hash2) % uint64(cms.rows))
	}
	return positions
}

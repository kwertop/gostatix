package count

import (
	"encoding/json"
	"errors"
	"math"
)

type CountMinSketch struct {
	AbstractCountMinSketch
	matrix [][]uint64
}

func NewCountMinSketch(rows, columns uint) (*CountMinSketch, error) {
	if rows <= 0 || columns <= 0 {
		return nil, errors.New("gostatix: rows and columns size should be greater than 0")
	}
	abstractSketch := MakeAbstractCountMinSketch(rows, columns, 0)
	matrix := make([][]uint64, rows)
	for i := range matrix {
		matrix[i] = make([]uint64, columns)
	}
	sketch := &CountMinSketch{*abstractSketch, matrix}
	return sketch, nil
}

func NewCountMinSketchFromEsitmates(errorRate, accuracy float64) (*CountMinSketch, error) {
	columns := uint(math.Ceil(math.E / errorRate))
	rows := uint(math.Ceil(math.Log(1 / accuracy)))
	return NewCountMinSketch(rows, columns)
}

func (cms *CountMinSketch) Update(data []byte, count uint64) {
	for r, c := range cms.getPositions(data) {
		cms.matrix[r][c] += count
	}
	cms.allSum += count
}

func (cms *CountMinSketch) UpdateString(data string, count uint64) {
	cms.Update([]byte(data), count)
}

func (cms *CountMinSketch) Count(data []byte) uint64 {
	var min uint64
	for r, c := range cms.getPositions(data) {
		if r == 0 || cms.matrix[r][c] < min {
			min = cms.matrix[r][c]
		}
	}
	return min
}

func (cms *CountMinSketch) CountString(data string) uint64 {
	return cms.Count([]byte(data))
}

type countMinSketchJSON struct {
	Rows    uint       `json:"r"`
	Columns uint       `json:"c"`
	AllSum  uint64     `json:"s"`
	Matrix  [][]uint64 `json:"m"`
}

func (cms *CountMinSketch) Export() ([]byte, error) {
	return json.Marshal(countMinSketchJSON{cms.rows, cms.columns, cms.allSum, cms.matrix})
}

func (cms *CountMinSketch) Import(data []byte) error {
	var s countMinSketchJSON
	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}
	cms.rows = s.Rows
	cms.columns = s.Columns
	cms.allSum = s.AllSum
	cms.matrix = s.Matrix
	return nil
}

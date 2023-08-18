package count

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sync"
)

type CountMinSketch struct {
	AbstractCountMinSketch
	matrix [][]uint64
	lock   sync.RWMutex
}

func NewCountMinSketch(rows, columns uint) (*CountMinSketch, error) {
	if rows <= 0 || columns <= 0 {
		return nil, fmt.Errorf("gostatix: rows and columns size should be greater than 0")
	}
	abstractSketch := MakeAbstractCountMinSketch(rows, columns, 0)
	matrix := make([][]uint64, rows)
	for i := range matrix {
		matrix[i] = make([]uint64, columns)
	}
	sketch := &CountMinSketch{AbstractCountMinSketch: *abstractSketch, matrix: matrix}
	return sketch, nil
}

func NewCountMinSketchFromEstimates(errorRate, delta float64) (*CountMinSketch, error) {
	columns := uint(math.Ceil(math.E / errorRate))
	rows := uint(math.Ceil(math.Log(1 / delta)))
	return NewCountMinSketch(rows, columns)
}

func (cms *CountMinSketch) UpdateOnce(data []byte) {
	cms.Update(data, 1)
}

func (cms *CountMinSketch) Merge(cms1 *CountMinSketch) error {
	if cms.rows != cms1.rows {
		return fmt.Errorf("gostatix: can't merge sketches with unequal row counts, %d and %d", cms.rows, cms1.rows)
	}
	if cms.columns != cms1.columns {
		return fmt.Errorf("gostatix: can't merge sketches with unequal column counts, %d and %d", cms.columns, cms1.columns)
	}
	for i := range cms.matrix {
		for j := range cms.matrix[i] {
			cms.matrix[i][j] += cms1.matrix[i][j]
		}
	}
	return nil
}

func (cms *CountMinSketch) Update(data []byte, count uint64) {
	cms.lock.Lock()
	defer cms.lock.Unlock()

	for r, c := range cms.getPositions(data) {
		cms.matrix[r][c] += count
	}
	cms.allSum += count
}

func (cms *CountMinSketch) UpdateString(data string, count uint64) {
	cms.Update([]byte(data), count)
}

func (cms *CountMinSketch) Count(data []byte) uint64 {
	cms.lock.Lock()
	defer cms.lock.Unlock()

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
	Key     string     `json:"k"`
}

func (cms *CountMinSketch) Export() ([]byte, error) {
	return json.Marshal(countMinSketchJSON{cms.rows, cms.columns, cms.allSum, cms.matrix, ""})
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

func (cms *CountMinSketch) Equals(cms1 *CountMinSketch) bool {
	if cms.rows != cms1.rows && cms.columns != cms1.columns {
		return false
	}
	for i := range cms.matrix {
		for j := range cms.matrix[i] {
			if cms.matrix[i][j] != cms1.matrix[i][j] {
				return false
			}
		}
	}
	return true
}

func (cms *CountMinSketch) WriteTo(stream io.Writer) (int64, error) {
	err := binary.Write(stream, binary.BigEndian, uint64(cms.rows))
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, uint64(cms.columns))
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, cms.allSum)
	if err != nil {
		return 0, err
	}
	row := make([]uint64, cms.columns)
	for r := uint(0); r < cms.rows; r++ {
		for c := uint(0); c < cms.columns; c++ {
			row[c] = cms.matrix[r][c]
		}
		err = binary.Write(stream, binary.BigEndian, row)
		if err != nil {
			return 0, err
		}
	}
	return int64(3*binary.Size(uint64(0)) + int(cms.rows)*binary.Size(row)), nil
}

func (cms *CountMinSketch) ReadFrom(stream io.Reader) (int64, error) {
	var rows, columns, allSum uint64
	err := binary.Read(stream, binary.BigEndian, &rows)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &columns)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &allSum)
	if err != nil {
		return 0, err
	}
	cms.rows = uint(rows)
	cms.columns = uint(columns)
	cms.allSum = allSum
	cms.matrix = make([][]uint64, cms.rows)
	for r := uint(0); r < cms.rows; r++ {
		cms.matrix[r] = make([]uint64, cms.columns)
	}

	row := make([]uint64, cms.columns)
	for r := uint(0); r < cms.rows; r++ {
		err = binary.Read(stream, binary.BigEndian, &row)
		if err != nil {
			return 0, err
		}
		for c := uint(0); c < cms.columns; c++ {
			cms.matrix[r][c] = row[c]
		}
	}
	return int64(2*binary.Size(uint64(0)) + int(cms.rows)*binary.Size(row)), nil
}

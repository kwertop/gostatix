package count

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/kwertop/gostatix"
	"github.com/redis/go-redis/v9"
)

type CountMinSketchRedis struct {
	AbstractCountMinSketch
	key string
}

func NewCountMinSketchRedis(rows, columns uint) (*CountMinSketchRedis, error) {
	if rows <= 0 || columns <= 0 {
		return nil, errors.New("gostatix: rows and columns size should be greater than 0")
	}
	abstractSketch := MakeAbstractCountMinSketch(rows, columns, 0)
	key := gostatix.GenerateRandomString(16)
	sketch := &CountMinSketchRedis{*abstractSketch, key}
	sketch.initMatrix()
	return sketch, nil
}

func NewCountMinSketchRedisFromEsitmates(errorRate, accuracy float64) (*CountMinSketch, error) {
	columns := uint(math.Ceil(math.E / errorRate))
	rows := uint(math.Ceil(math.Log(1 / accuracy)))
	return NewCountMinSketch(rows, columns)
}

func (cms *CountMinSketchRedis) UpdateOnce(data []byte) {
	cms.Update(data, 1)
}

func (cms *CountMinSketchRedis) Update(data []byte, count uint64) error {
	updateLists := redis.NewScript(`
		local size = ARGV[1]
		local cmsKey = ARGV[2]
		for i=1, tonumber(size)-1, 2 do
			local row = cmsKey .. KEYS[i]
			local column = tonumber(KEYS[i+1])
			local val = redis.call('LINDEX, row, column)
			val = tonumber(val) + count
			redis.pcall('LSET', row, columns, val)
		end
		return true
	`)
	var updateRedisKeys []string
	for r, c := range cms.getPositions(data) {
		updateRedisKeys = append(updateRedisKeys, strconv.FormatInt(int64(r), 10), strconv.FormatUint(uint64(c), 10))
	}
	_, err := updateLists.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		updateRedisKeys,
		len(updateRedisKeys),
		cms.key,
	).Bool()
	if err != nil {
		return fmt.Errorf("gostatix: error while updating data %v in redis, error: %v", data, err)
	}
	cms.allSum += count
	return nil
}

func (cms *CountMinSketchRedis) UpdateString(data string, count uint64) error {
	return cms.Update([]byte(data), count)
}

func (cms *CountMinSketchRedis) Count(data []byte) (uint64, error) {
	countLists := redis.NewScript(`
		local size = ARGV[1]
		local cmsKey = ARGV[2]
		local min = 0
		for i=1, tonumber(size)-1, 2 do
			local row = cmsKey .. KEYS[i]
			local column = tonumber(KEYS[i+1])
			local val = redis.call('LINDEX, row, column)
			if val < min then
				val = min
			end
		end
		return min
	`)
	var countRedisKeys []string
	for r, c := range cms.getPositions(data) {
		countRedisKeys = append(countRedisKeys, strconv.FormatInt(int64(r), 10), strconv.FormatUint(uint64(c), 10))
	}
	minVal, err := countLists.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		countRedisKeys,
		len(countRedisKeys),
		cms.key,
	).Uint64()
	if err != nil {
		return 0, fmt.Errorf("gostatix: error while couting data %v in redis, error: %v", data, err)
	}
	return minVal, nil
}

func (cms *CountMinSketchRedis) CountString(data string) (uint64, error) {
	return cms.Count([]byte(data))
}

func (cms CountMinSketchRedis) initMatrix() error {
	rowKeys := make([]string, cms.rows)
	for i := range rowKeys {
		rowKeys[i] = cms.key + "_" + strconv.FormatInt(int64(i), 10)
	}
	initMatrixRedis := redis.NewScript(`
		local rows = ARGV[1]
		local columns = ARGV[2]
		for i=1, tonumber(rows) do
			local rowKey = KEYS[i]
			redis.call('DEL', rowKey)
			local list = {}
			for j=1, tonumber(columns) do
				list[j] = 0
			end
			redis.call('LPUSH', rowKey, list)
		end
		return true
	`)
	ok, err := initMatrixRedis.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		rowKeys,
		cms.rows,
		cms.columns,
	).Bool()
	if err != nil || !ok {
		return errors.New("gostatix: error while initializing matrix in redis")
	}
	return nil
}

func (cms *CountMinSketchRedis) getMatrix() ([][]uint64, error) {
	fetchMatrixAsTable := redis.NewScript(`
		local key = KEYS[1]
		local size = ARGV[1]
		local matrix = {}
		for i=1, size do
			matrix[i] = {}
			local rowKey = key .. tostring(i)
			local values = redis.call('LRANGE', rowKey, 0, -1)
			for j, v in ipairs(values) do
				matrix[i][j] = v
			end
		end
		return matrix
	`)
	result, err := fetchMatrixAsTable.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		[]string{cms.key},
		cms.rows,
	).Result()
	if err != nil {
		return nil, fmt.Errorf("gostatix: error fetching matrix from redis, error: %v", err)
	}
	matrix, ok := result.([][]uint64)
	if ok {
		return matrix, nil
	} else {
		return nil, fmt.Errorf("gostatix: error parsing matrix from redis")
	}
}

func (cms *CountMinSketchRedis) setMatrix(matrix [][]uint64) error {
	fetchMatrixAsTable := redis.NewScript(`
		local key = KEYS[1]
		for i,v in ipairs(ARGV) do
			local rowKey = key .. tostring(i)
			redis.call('LPUSH', rowKey, v)
		end
		return true
	`)
	_, err := fetchMatrixAsTable.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		[]string{cms.key},
		matrix,
	).Result()
	if err != nil {
		return fmt.Errorf("gostatix: couldn't save matrix in redis")
	}
	return nil
}

type countMinSketchRedisJSON struct {
	Rows    uint       `json:"r"`
	Columns uint       `json:"c"`
	AllSum  uint64     `json:"s"`
	Matrix  [][]uint64 `json:"m"`
	Key     string     `json:"k"`
}

func (cms *CountMinSketchRedis) Export() ([]byte, error) {
	matrix, err := cms.getMatrix()
	if err != nil {
		return nil, err
	}
	return json.Marshal(countMinSketchJSON{cms.rows, cms.columns, cms.allSum, matrix})
}

func (cms *CountMinSketchRedis) Import(data []byte) error {
	var s countMinSketchRedisJSON
	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}
	cms.rows = s.Rows
	cms.columns = s.Columns
	cms.allSum = s.AllSum
	return cms.setMatrix(s.Matrix)
}

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

func NewCountMinSketchRedisFromEstimates(errorRate, delta float64) (*CountMinSketchRedis, error) {
	columns := uint(math.Ceil(math.E / errorRate))
	rows := uint(math.Ceil(math.Log(1 / delta)))
	return NewCountMinSketchRedis(rows, columns)
}

func (cms *CountMinSketchRedis) UpdateOnce(data []byte) {
	cms.Update(data, 1)
}

func (cms *CountMinSketchRedis) Update(data []byte, count uint64) error {
	updateLists := redis.NewScript(`
		local size = ARGV[1]
		local cmsKey = ARGV[2]
		local count = tonumber(ARGV[3])
		for i=1, tonumber(size)-1, 2 do
			local row = cmsKey .. KEYS[i]
			local column = tonumber(KEYS[i+1])
			local val = redis.call('LINDEX', row, column)
			val = tonumber(val) + count
			redis.pcall('LSET', row, column, val)
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
		count,
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
			local val = redis.call('LINDEX', row, column)
			local count = tonumber(val)
			if count < min or tonumber(KEYS[i]) == 0 then
				min = count
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

func (cms *CountMinSketchRedis) Merge(cms1 *CountMinSketchRedis) error {
	if cms.rows != cms1.rows {
		return fmt.Errorf("gostatix: can't merge sketches with unequal row counts, %d and %d", cms.rows, cms1.rows)
	}
	if cms.columns != cms1.columns {
		return fmt.Errorf("gostatix: can't merge sketches with unequal column counts, %d and %d", cms.columns, cms1.columns)
	}
	return cms.mergeMatrix(cms1.key)
}

func (cms *CountMinSketchRedis) Equals(cms1 *CountMinSketchRedis) (bool, error) {
	if cms.rows != cms1.rows || cms.columns != cms1.columns {
		return false, nil
	}
	return cms.compareMatrix(cms1.key)
}

func (cms *CountMinSketchRedis) compareMatrix(key string) (bool, error) {
	compareMatrixScript := redis.NewScript(`
		local key1 = KEYS[1]
		local key2 = KEYS[2]
		local rows = tonumber(ARGV[1])
		local columns = tonumber(ARGV[2])
		for i=1, tonumber(rows) do
			local rowKey1 = key1 .. tostring(i-1)
			local vals1 = redis.pcall('LRANGE', rowKey1, 0, -1)
			local rowKey2 = key2 .. tostring(i-1)
			local vals2 = redis.pcall('LRANGE', rowKey2, 0, -1)
			for j=1, tonumber(columns) do
				if vals1[j] ~= vals2[j] then
					return false
				end
			end
		end
		return true
	`)
	ok, err := compareMatrixScript.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		[]string{cms.key, key},
		cms.rows,
		cms.columns,
	).Bool()
	if err != nil || !ok {
		return false, fmt.Errorf("gostatix: error while comparing matrix in redis")
	}
	return ok, nil
}

func (cms *CountMinSketchRedis) mergeMatrix(key string) error {
	mergeMatrixScript := redis.NewScript(`
		local key1 = KEYS[1]
		local key2 = KEYS[2]
		local rows = tonumber(ARGV[1])
		local columns = tonumber(ARGV[2])
		for i=1, tonumber(rows) do
			local rowKey1 = key1 .. tostring(i-1)
			local vals1 = redis.call('LRANGE', rowKey1, 0, -1)
			local rowKey2 = key2 .. tostring(i-1)
			local vals2 = redis.call('LRANGE', rowKey2, 0, -1)
			local vals3 = {}
			for j=1, tonumber(columns) do
				vals3[j] = tonumber(vals1[j]) + tonumber(vals2[j])
			end
			redis.call('DEL', rowKey1)
			redis.call('RPUSH', rowKey1, unpack(vals3))
		end
		return true
	`)
	ok, err := mergeMatrixScript.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		[]string{cms.key, key},
		cms.rows,
		cms.columns,
	).Bool()
	if err != nil || !ok {
		return errors.New("gostatix: error while merging matrix in redis")
	}
	return nil
}

func (cms CountMinSketchRedis) initMatrix() error {
	rowKeys := make([]string, cms.rows)
	for i := range rowKeys {
		rowKeys[i] = cms.key + "_" + strconv.FormatInt(int64(i), 10)
	}
	initMatrixRedis := redis.NewScript(`
		local key = KEYS[1]
		local rows = ARGV[1]
		local columns = ARGV[2]
		for i=1, tonumber(rows) do
			local rowKey = key .. tostring(i-1)
			redis.call('DEL', rowKey)
			local list = {}
			for j=1, tonumber(columns) do
				list[j] = 0
			end
			redis.call('LPUSH', rowKey, unpack(list))
		end
		return true
	`)
	ok, err := initMatrixRedis.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		[]string{cms.key},
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
		for i=1, tonumber(size) do
			matrix[i] = {}
			local rowKey = key .. tostring(i-1)
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
	).Slice()
	if err != nil {
		return nil, fmt.Errorf("gostatix: error fetching matrix from redis, error: %v", err)
	}

	matrix := make([][]uint64, len(result))
	for i := range result {
		rowSlice, ok := result[i].([]interface{})
		matrix[i] = make([]uint64, len(rowSlice))
		if ok {
			for j := range rowSlice {
				c, ok := rowSlice[j].(string)
				if ok {
					count, err := strconv.Atoi(c)
					if err != nil {
						return nil, fmt.Errorf("gostatix: error parsing matrix from redis, error: %v", err)
					}
					matrix[i][j] = uint64(count)
				}
			}
		} else {
			return nil, fmt.Errorf("gostatix: error parsing matrix from redis")
		}
	}
	return matrix, nil
}

func (cms *CountMinSketchRedis) setMatrix(matrix [][]uint64) error {
	flattenedMatrix := gostatix.Flatten(matrix)
	args := make([]interface{}, len(flattenedMatrix)+1)
	args[0] = interface{}(len(matrix[0]))
	for i := range flattenedMatrix {
		args[i+1] = interface{}(flattenedMatrix[i])
	}
	setMatrixScript := redis.NewScript(`
		local key = KEYS[1]
		local columns = tonumber(ARGV[1])
		local index = 2
		local rows = #ARGV / columns
		for i=1, rows do
			local row = {}
			local rowKey = key .. tostring(i-1)
			for j=1, columns do
				row[j] = ARGV[index]
				index = index + 1
			end
			redis.call('RPUSH', rowKey, unpack(row))
		end
		return true
	`)
	_, err := setMatrixScript.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		[]string{cms.key},
		args...,
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
	return json.Marshal(countMinSketchRedisJSON{cms.rows, cms.columns, cms.allSum, matrix, cms.key})
}

func (cms *CountMinSketchRedis) Import(data []byte, withNewKey bool) error {
	var s countMinSketchRedisJSON
	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}
	cms.rows = s.Rows
	cms.columns = s.Columns
	cms.allSum = s.AllSum
	if withNewKey {
		cms.key = gostatix.GenerateRandomString(16)
	} else {
		cms.key = s.Key
	}
	return cms.setMatrix(s.Matrix)
}
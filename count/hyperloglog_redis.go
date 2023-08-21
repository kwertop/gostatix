/*
Package count implements various probabilistic data structures used in counting.

 1. Count-Min Sketch: A probabilistic data structure used to estimate the frequency
    of items in a data stream. Refer: http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf
 2. Hyperloglog: A probabilistic data structure used for estimating the cardinality
    (number of unique elements) of in a very large dataset.
    Refer: https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
 3. Top-K: A data structure is designed to efficiently retrieve the "top-K" or "largest-K"
    elements from a dataset based on a certain criterion, such as frequency, value, or score

The package implements both in-mem and Redis backed solutions for the data structures. The
in-memory data structures are thread-safe.
*/
package count

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/kwertop/gostatix"
	"github.com/redis/go-redis/v9"
)

// HyperLogLogRedis is the Redis backed implementation of BaseHyperLogLog
// _key_ holds the Redis key to the list which has the registers
// _metadataKey_ is used to store the additional information about HyperLogLogRedis
// for retrieving the sketch by the Redis key
type HyperLogLogRedis struct {
	AbstractHyperLogLog
	key         string
	metadataKey string
}

// NewHyperLogLogRedis creates new HyperLogLogRedis with the specified _numRegisters_
func NewHyperLogLogRedis(numRegisters uint64) (*HyperLogLogRedis, error) {
	abstractLog, err := MakeAbstractHyperLogLog(numRegisters)
	if err != nil {
		return nil, err
	}
	key := gostatix.GenerateRandomString(16)
	metadataKey := gostatix.GenerateRandomString(16)
	h := &HyperLogLogRedis{*abstractLog, key, metadataKey}
	metadata := make(map[string]interface{})
	metadata["numRegisters"] = h.numRegisters
	metadata["key"] = h.key
	err = gostatix.GetRedisClient().HSet(context.Background(), h.metadataKey, metadata).Err()
	if err != nil {
		return nil, fmt.Errorf("gostatix: error creating count min sketch redis, error: %v", err)
	}
	err = h.initRegisters()
	if err != nil {
		return nil, err
	}
	return h, nil
}

// NewHyperLogLogRedisFromKey is used to create a new Redis backed HyperLogLogRedis from the
// _metadataKey_ (the Redis key used to store the metadata about the hyperloglog) passed.
// For this to work, value should be present in Redis at _key_
func NewHyperLogLogRedisFromKey(metadataKey string) (*HyperLogLogRedis, error) {
	values, err := gostatix.GetRedisClient().HGetAll(context.Background(), metadataKey).Result()
	if err != nil {
		return nil, fmt.Errorf("gostatix: error creating hyeprloglog from redis key, error: %v", err)
	}
	numRegisters, _ := strconv.Atoi(values["numRegisters"])
	abstractLog, err := MakeAbstractHyperLogLog(uint64(numRegisters))
	if err != nil {
		return nil, err
	}
	h := &HyperLogLogRedis{*abstractLog, values["key"], metadataKey}
	return h, nil
}

// MetadataKey returns the metadataKey
func (h *HyperLogLogRedis) MetadataKey() string {
	return h.metadataKey
}

// Update sets the count of the passed _data_ (byte slice) to the hashed location
// in the Redis list at _key_
func (h *HyperLogLogRedis) Update(data []byte) error {
	registerIndex, count := h.getRegisterIndexAndCount(data)
	return h.updateRegisters(uint8(registerIndex), uint8(count))
}

// Count returns the number of distinct elements so far
// _withCorrection_ is used to specify if correction is to be done for large registers
// _withRoundingOff_ is used to specify if rounding off is required for estimation
func (h *HyperLogLogRedis) Count(withCorrection bool, withRoundingOff bool) (uint64, error) {
	harmonicMean, err := h.computeHarmonicMean()
	if err != nil {
		return 0, err
	}
	return h.getEstimation(harmonicMean, withCorrection, withRoundingOff), nil
}

// Merge merges two HyperLogLogRedis data structures
func (h *HyperLogLogRedis) Merge(g *HyperLogLogRedis) error {
	if h.numRegisters != g.numRegisters {
		return fmt.Errorf("gostatix: number of registers %d, %d don't match", h.numRegisters, g.numRegisters)
	}
	return h.mergeRegisters(g.key)
}

// Equals checks if two HyperLogLogRedis data structures are equal
func (h *HyperLogLogRedis) Equals(g *HyperLogLogRedis) (bool, error) {
	if h.numRegisters != g.numRegisters {
		return false, nil
	}
	return h.compareRegisters(g.key)
}

// Export JSON marshals the HyperLogLogRedis and returns a byte slice containing the data
func (h *HyperLogLogRedis) Export() ([]byte, error) {
	result, err := gostatix.GetRedisClient().LRange(
		context.Background(),
		h.key,
		0,
		-1,
	).Result()
	if err != nil {
		return nil, fmt.Errorf("gostatix: error fetching registers from redis, error: %v", err)
	}
	registers := make([]uint8, h.numRegisters)
	for i := range registers {
		val, _ := strconv.Atoi(result[i])
		registers[i] = uint8(val)
	}
	return json.Marshal(hyperLogLogJSON{h.numRegisters, h.numBytesPerHash, h.correctionBias, registers, h.key})
}

// Import JSON unmarshals the _data_ into the HyperLogLogRedis
func (h *HyperLogLogRedis) Import(data []byte, withNewKey bool) error {
	var g hyperLogLogJSON
	err := json.Unmarshal(data, &g)
	if err != nil {
		return err
	}
	h.numRegisters = g.NumRegisters
	h.numBytesPerHash = g.NumBytesPerHash
	h.correctionBias = g.CorrectionBias
	if withNewKey {
		h.key = gostatix.GenerateRandomString(16)
	} else {
		h.key = g.Key
	}
	return h.importRegisters(g.Registers)
}

func (h *HyperLogLogRedis) importRegisters(registers []uint8) error {
	args := make([]interface{}, len(registers))
	for i := range registers {
		args[i] = interface{}(registers[i])
	}
	importRegistersScript := redis.NewScript(`
		local key = KEYS[1]
		local size = #ARGV
		local registers = {}
		for i=1, size do
			registers[i] = tonumber(ARGV[i])
		end
		redis.call('RPUSH', key, unpack(registers))
		return true
	`)
	_, err := importRegistersScript.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		[]string{h.key},
		args...,
	).Bool()
	if err != nil {
		return fmt.Errorf("gostatix: error importing registers for key: %s, error: %v", h.key, err)
	}
	return nil
}

func (h *HyperLogLogRedis) mergeRegisters(key string) error {
	mergeRegistersScript := redis.NewScript(`
		local key1 = KEYS[1]
		local key2 = KEYS[2]
		local size = ARGV[1]
		local vals1 = redis.pcall('LRANGE', key1, 0, -1)
		local vals2 = redis.pcall('LRANGE', key2, 0, -1)
		for i=1, tonumber(size) do
			if tonumber(vals1[i]) < tonumber(vals2[i]) then
				vals1[i] = vals2[i]
			end
		end
		redis.pcall('LPUSH', key1, unpack(vals1))
		return true
	`)
	_, err := mergeRegistersScript.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		[]string{h.key, key},
		h.numRegisters,
	).Bool()
	if err != nil {
		return fmt.Errorf("gostatix: error while merging registers %s with %s, error: %v", h.key, key, err)
	}
	return nil
}

func (h *HyperLogLogRedis) compareRegisters(key string) (bool, error) {
	equals := redis.NewScript(`
		local key1 = KEYS[1]
		local key2 = KEYS[2]
		local size = ARGV[1]
		local vals1 = redis.pcall('LRANGE', key1, 0, -1)
		local vals2 = redis.pcall('LRANGE', key2, 0, -1)
		for i=1, tonumber(size) do
			if tonumber(vals1[i]) ~= tonumber(vals2[i]) then
				return false
			end
		end
		return true
	`)
	ok, err := equals.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		[]string{h.key, key},
		h.numRegisters,
	).Bool()
	if err != nil {
		return false, fmt.Errorf("gostatix: error while comparing registers %s with %s, error: %v", h.key, key, err)
	}
	return ok, nil
}

func (h *HyperLogLogRedis) computeHarmonicMean() (float64, error) {
	harmonicMeanScript := redis.NewScript(`
		local key = KEYS[1]
		local size = ARGV[1]
		local hmean = 0.0
		local values = redis.pcall('LRANGE', key, 0, -1)
		for i=1, tonumber(size) do
			local value = (-1)*tonumber(values[i])
			hmean = hmean + 2^(value)
		end
		return hmean
	`)
	hmean, err := harmonicMeanScript.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		[]string{h.key},
		h.numRegisters,
	).Float64()
	if err != nil {
		return 0, fmt.Errorf("gostatix: error while computing harmonic mean of hyperloglog, error: %v", err)
	}
	return hmean, nil
}

func (h *HyperLogLogRedis) updateRegisters(index, count uint8) error {
	updateList := redis.NewScript(`
		local key = KEYS[1]
		local index = tonumber(ARGV[1])
		local val = tonumber(ARGV[2])
		local count = redis.call('LINDEX', key, index)
		if val > tonumber(count) then
			count = val
		end
		redis.call('LSET', key, index, count)
		return true
	`)
	_, err := updateList.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		[]string{h.key},
		index,
		count,
	).Bool()
	if err != nil {
		return fmt.Errorf("gostatix: error while updating hyperloglog registers in redis, error: %v", err)
	}
	return nil
}

func (h *HyperLogLogRedis) initRegisters() error {
	initList := redis.NewScript(`
		local key = KEYS[1]
		local size = ARGV[1]
		local registers = {}
		for i=1, tonumber(size)/2 do
			registers[i] = 0
		end
		redis.call('LPUSH', key, unpack(registers))
		redis.call('LPUSH', key, unpack(registers))
		return true
	`)
	_, err := initList.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		[]string{h.key},
		h.numRegisters,
	).Bool()
	if err != nil {
		return fmt.Errorf("gostatix: error while initializing hyperloglog registers in redis, error: %v", err)
	}
	return nil
}

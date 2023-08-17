package count

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/kwertop/gostatix"
	"github.com/redis/go-redis/v9"
)

type HyperLogLogRedis struct {
	AbstractHyperLogLog
	key string
}

func NewHyperLogLogRedis(numRegisters uint64) (*HyperLogLogRedis, error) {
	abstractLog, err := MakeAbstractHyperLogLog(numRegisters)
	if err != nil {
		return nil, err
	}
	key := gostatix.GenerateRandomString(16)
	h := &HyperLogLogRedis{*abstractLog, key}
	err = h.initRegisters()
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (h *HyperLogLogRedis) Update(data []byte) error {
	registerIndex, count := h.getRegisterIndexAndCount(data)
	return h.updateRegisters(uint8(registerIndex), uint8(count))
}

func (h *HyperLogLogRedis) Count(withCorrection bool, withRoundingOff bool) (uint64, error) {
	harmonicMean, err := h.computeHarmonicMean()
	if err != nil {
		return 0, err
	}
	return h.getEstimation(harmonicMean, withCorrection, withRoundingOff), nil
}

func (h *HyperLogLogRedis) Equals(g *HyperLogLogRedis) (bool, error) {
	if h.numRegisters != g.numRegisters {
		return false, nil
	}
	return h.compareRegisters(g.key)
}

func (h *HyperLogLogRedis) Merge(g *HyperLogLogRedis) error {
	if h.numRegisters != g.numRegisters {
		return fmt.Errorf("gostatix: number of registers %d, %d don't match", h.numRegisters, g.numRegisters)
	}
	return h.mergeRegisters(g.key)
}

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

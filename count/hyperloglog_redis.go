package count

import (
	"context"
	"fmt"

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

func (h *HyperLogLogRedis) initRegisters() error {
	initList := redis.NewScript(`
		local key = KEYS[1]
		local size = ARGV[1]
		local registers = {}
		for i=1, tonumber(size) do
			registers[i] = 0
		end
		redis.call('LPUSH', key, registers)
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

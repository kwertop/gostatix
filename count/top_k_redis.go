package count

import (
	"context"
	"sort"

	"github.com/kwertop/gostatix"
	"github.com/redis/go-redis/v9"
)

type TopKRedis struct {
	k         uint
	errorRate float64
	accuracy  float64
	sketch    CountMinSketchRedis
	heapKey   string
}

func NewTopKRedis(k uint, errorRate, accuracy float64) *TopKRedis {
	sketch, _ := NewCountMinSketchRedisFromEstimates(errorRate, accuracy)
	heapKey := gostatix.GenerateRandomString(16)
	return &TopKRedis{k, errorRate, accuracy, *sketch, heapKey}
}

func (t *TopKRedis) Insert(data []byte, count uint64) error {
	element := string(data)
	if count <= 0 {
		panic("count must be greater than zero")
	}
	t.sketch.Update(data, count)
	frequency, err := t.sketch.Count(data)
	if err != nil {
		return err
	}
	heapLength, err := gostatix.GetRedisClient().ZCard(context.Background(), t.heapKey).Uint64()
	if err != nil {
		return err
	}
	minElement, err := gostatix.GetRedisClient().ZRangeWithScores(context.Background(), t.heapKey, 0, 0).Result()
	if err != nil {
		return err
	}
	if heapLength < uint64(t.k) || frequency >= uint64(minElement[0].Score) {
		index, err := gostatix.GetRedisClient().ZScore(context.Background(), t.heapKey, element).Result()
		if err != nil {
			return err
		}
		if index > -1 {
			err := gostatix.GetRedisClient().ZRem(context.Background(), t.heapKey, element).Err()
			if err != nil {
				return err
			}
		}
		err = gostatix.GetRedisClient().ZAdd(
			context.Background(),
			t.heapKey,
			redis.Z{Score: float64(count), Member: element},
		).Err()
		if err != nil {
			return err
		}
		heapLength, err = gostatix.GetRedisClient().ZCard(context.Background(), t.heapKey).Uint64()
		if err != nil {
			return err
		}
		if heapLength > uint64(t.k) {
			err := gostatix.GetRedisClient().ZPopMin(context.Background(), t.heapKey).Err()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *TopKRedis) Values() ([]TopKElement, error) {
	var results []TopKElement
	elements, err := gostatix.GetRedisClient().ZRangeWithScores(context.Background(), t.heapKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	sort.Slice(elements, func(i, j int) bool {
		return elements[i].Score < elements[j].Score
	})
	for i := len(elements) - 1; i >= 0; i-- {
		results = append(results, TopKElement{elements[i].Member.(string), uint64(elements[i].Score)})
	}
	return results, nil
}

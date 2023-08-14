package count

import (
	"context"
	"encoding/json"
	"fmt"
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

func (t *TopKRedis) Equals(u *TopKRedis) (bool, error) {
	if t.k != u.k {
		return false, fmt.Errorf("parameter k are not equal, %d and %d", t.k, u.k)
	}
	if t.accuracy != u.accuracy {
		return false, fmt.Errorf("parameter accuracy are not equal, %f and %f", t.accuracy, u.accuracy)
	}
	if t.errorRate != u.errorRate {
		return false, fmt.Errorf("parameter errorRate are not equal, %f and %f", t.errorRate, u.errorRate)
	}
	if ok, _ := t.sketch.Equals(&u.sketch); !ok {
		return false, fmt.Errorf("sketches aren't equal")
	}
	return t.compareHeaps(u.heapKey)
}

func (t *TopKRedis) Export() ([]byte, error) {
	result, err := gostatix.GetRedisClient().ZRangeWithScores(
		context.Background(),
		t.heapKey,
		0,
		-1,
	).Result()
	if err != nil {
		return nil, fmt.Errorf("gostatix: error fetching heap from redis, error: %v", err)
	}
	var sketch countMinSketchJSON
	sketch.AllSum = t.sketch.allSum
	sketch.Columns = t.sketch.columns
	sketch.Rows = t.sketch.rows
	sketch.Matrix, _ = t.sketch.getMatrix()
	var heap []heapElementJSON
	for i := range result {
		heap = append(heap, heapElementJSON{Value: result[i].Member.(string), Frequency: uint64(result[i].Score)})
	}
	return json.Marshal(topKJSON{t.k, t.errorRate, t.accuracy, sketch, heap, t.heapKey})
}

func (t *TopKRedis) Import(data []byte, withNewKey bool) error {
	var topk topKJSON
	err := json.Unmarshal(data, &topk)
	if err != nil {
		return fmt.Errorf("gostatix: error while unmarshalling data, error %v", err)
	}
	t.k = topk.K
	t.accuracy = topk.Accuracy
	t.errorRate = topk.ErrorRate
	if withNewKey {
		t.heapKey = gostatix.GenerateRandomString(16)
	} else {
		t.heapKey = topk.HeapKey
	}
	frequencyMap := make(map[string]uint)
	for i := range topk.Heap {
		frequencyMap[topk.Heap[i].Value]++
	}
	err = t.importHeap(t.heapKey, frequencyMap)
	if err != nil {
		return fmt.Errorf("gostatix: error while unmarshalling data, error %v", err)
	}
	sketch, err := NewCountMinSketchRedis(topk.Sketch.Rows, topk.Sketch.Columns)
	if err != nil {
		return fmt.Errorf("gostatix: error while unmarshalling data, error %v", err)
	}
	sketch.allSum = topk.Sketch.AllSum
	sketch.setMatrix(topk.Sketch.Matrix)
	t.sketch = *sketch
	return nil
}

func (t *TopKRedis) importHeap(key string, frequencyMap map[string]uint) error {
	args := make([]interface{}, 2*len(frequencyMap))
	i := 0
	for key, val := range frequencyMap {
		args[i] = interface{}(key)
		args[i+1] = interface{}(val)
		i = i + 2
	}
	importHeapScript := redis.NewScript(`
		local key = KEYS[1]
		local vals2 = redis.pcall('ZRANGE', key2, 0, -1)
		for i=1, #ARGV, 2 do
			local element = ARGV[i]
			local score = ARGV[i+1]
			redis.call('ZADD', key, score, element)
		end
		return true
	`)
	_, err := importHeapScript.Run(
		context.Background(),
		gostatix.GetRedisClient(),
		[]string{t.heapKey},
		args...,
	).Bool()
	if err != nil {
		return fmt.Errorf("gostatix: error importing registers for key: %s, error: %v", t.heapKey, err)
	}
	return nil
}

func (t *TopKRedis) compareHeaps(key string) (bool, error) {
	equals := redis.NewScript(`
		local key1 = KEYS[1]
		local key2 = KEYS[2]
		local size = ARGV[1]
		local vals1 = redis.pcall('ZRANGE', key1, 0, -1)
		local vals2 = redis.pcall('ZRANGE', key2, 0, -1)
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
		[]string{t.heapKey, key},
		t.k,
	).Bool()
	if err != nil {
		return false, fmt.Errorf("gostatix: error while comparing heaps %s with %s, error: %v", t.heapKey, key, err)
	}
	return ok, nil
}

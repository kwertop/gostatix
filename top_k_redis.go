/*
Package count implements various probabilistic data structures used in estimating top-K elements.

Top-K: A data structure designed to efficiently retrieve the "top-K" or "largest-K"
elements from a dataset based on a certain criterion, such as frequency, value, or score

The package implements both in-mem and Redis backed solutions for the data structures. The
in-memory data structures are thread-safe.
*/
package gostatix

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/kwertop/gostatix/internal/util"
	"github.com/redis/go-redis/v9"
)

// In-memory TopKRedis struct.
// _k_ is the number of top elements to track
// _errorRate_ is the acceptable error rate in topk estimation
// _accuracy_ is the delta in the error rate
// _sketch_ is the redis backed count-min sketch used to keep the estimated track of counts
// _heapKey_ is a key to Redis sorted set
// _metadataKey_ is used to store the additional information about TopKRedis
type TopKRedis struct {
	k           uint
	errorRate   float64
	accuracy    float64
	sketch      *CountMinSketchRedis
	heapKey     string
	metadataKey string
}

// NewTopKRedis creates new TopKRedis
// _k_ is the number of top elements to track
// _errorRate_ is the acceptable error rate in topk estimation
// _accuracy_ is the delta in the error rate
func NewTopKRedis(k uint, errorRate, accuracy float64) *TopKRedis {
	sketch, _ := NewCountMinSketchRedisFromEstimates(errorRate, accuracy)
	heapKey := util.GenerateRandomString(16)
	metadataKey := util.GenerateRandomString(16)
	metadata := make(map[string]interface{})
	metadata["k"] = k
	metadata["heapKey"] = heapKey
	metadata["errorRate"] = errorRate
	metadata["accuracy"] = accuracy
	metadata["sketchKey"] = sketch.MetadataKey()
	err := getRedisClient().HSet(context.Background(), metadataKey, metadata).Err()
	if err != nil {
		return nil
	}
	return &TopKRedis{k, errorRate, accuracy, sketch, heapKey, metadataKey}
}

// NewTopKRedisFromKey is used to create a new Redis backed TopKRedis from the
// _metadataKey_ (the Redis key used to store the metadata about the TopK) passed.
// For this to work, value should be present in Redis at _heapKey_
func NewTopKRedisFromKey(metadataKey string) *TopKRedis {
	values, _ := getRedisClient().HGetAll(context.Background(), metadataKey).Result()
	k, _ := strconv.ParseUint(values["k"], 10, 32)
	errorRate, _ := strconv.ParseFloat(values["errorRate"], 64)
	accuracy, _ := strconv.ParseFloat(values["accuracy"], 64)
	sketch, _ := NewCountMinSketchRedisFromKey(values["sketchKey"])
	heapKey := values["heapKey"]
	return &TopKRedis{uint(k), errorRate, accuracy, sketch, heapKey, metadataKey}
}

// MetadataKey returns the metadataKey
func (t *TopKRedis) MetadataKey() string {
	return t.metadataKey
}

// Insert puts the _data_ (byte slice) in the TopKRedis data structure with _count_
// _data_ is the element to be inserted
// _count_ is the count of the element
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
	heapLength, err := getRedisClient().ZCard(context.Background(), t.heapKey).Uint64()
	if err != nil {
		return err
	}
	minElement, err := getRedisClient().ZRangeWithScores(context.Background(), t.heapKey, 0, 0).Result()
	if err != nil {
		return err
	}
	if heapLength < uint64(t.k) || (len(minElement) > 0 && frequency >= uint64(minElement[0].Score)) {
		index := getRedisClient().ZScore(context.Background(), t.heapKey, element).Val()
		if index > 0 {
			err := getRedisClient().ZRem(context.Background(), t.heapKey, element).Err()
			if err != nil {
				return err
			}
		}
		err = getRedisClient().ZAdd(
			context.Background(),
			t.heapKey,
			redis.Z{Score: float64(frequency), Member: element},
		).Err()
		if err != nil {
			return err
		}
		heapLength, err = getRedisClient().ZCard(context.Background(), t.heapKey).Uint64()
		if err != nil {
			return err
		}
		if heapLength > uint64(t.k) {
			err := getRedisClient().ZPopMin(context.Background(), t.heapKey).Err()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Values returns the top _k_ elements in the TopKRedis data structure
func (t *TopKRedis) Values() ([]TopKElement, error) {
	var results []TopKElement
	elements, err := getRedisClient().ZRangeWithScores(context.Background(), t.heapKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	for i := len(elements) - 1; i >= 0; i-- {
		results = append(results, TopKElement{elements[i].Member.(string), uint64(elements[i].Score)})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].count == results[j].count {
			c := strings.Compare(results[i].element, results[j].element)
			if c == -1 {
				return true
			}
			if c == 1 {
				return false
			}
		}
		return results[i].count > results[j].count
	})
	return results, nil
}

// Equals checks if two TopKRedis structures are equal
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

	if ok, _ := t.sketch.Equals(u.sketch); !ok {
		return false, fmt.Errorf("sketches aren't equal")
	}
	return t.compareHeaps(u.heapKey)
}

// Export JSON marshals the TopKRedis and returns a byte slice containing the data
func (t *TopKRedis) Export() ([]byte, error) {
	result, err := getRedisClient().ZRangeWithScores(
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
	sketch.Key = t.sketch.key
	var heap []heapElementJSON
	for i := range result {
		heap = append(heap, heapElementJSON{Value: result[i].Member.(string), Frequency: uint64(result[i].Score)})
	}
	return json.Marshal(topKJSON{t.k, t.errorRate, t.accuracy, sketch, heap, t.heapKey})
}

// Import JSON unmarshals the _data_ into the TopKRedis
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
		t.heapKey = util.GenerateRandomString(16)
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
	t.sketch = sketch
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
		local vals2 = redis.pcall('ZRANGE', key, 0, -1)
		for i=1, #ARGV, 2 do
			local element = ARGV[i]
			local score = ARGV[i+1]
			redis.call('ZADD', key, score, element)
		end
		return true
	`)
	_, err := importHeapScript.Run(
		context.Background(),
		getRedisClient(),
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
		getRedisClient(),
		[]string{t.heapKey, key},
		t.k,
	).Bool()
	if err != nil {
		return false, fmt.Errorf("gostatix: error while comparing heaps %s with %s, error: %v", t.heapKey, key, err)
	}
	return ok, nil
}

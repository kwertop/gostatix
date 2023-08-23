/*
Package count implements various probabilistic data structures used in counting.

 1. Count-Min Sketch: A probabilistic data structure used to estimate the frequency
    of items in a data stream. Refer: http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf
 2. Hyperloglog: A probabilistic data structure used for estimating the cardinality
    (number of unique elements) of in a very large dataset.
    Refer: https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
 3. Top-K: A data structure designed to efficiently retrieve the "top-K" or "largest-K"
    elements from a dataset based on a certain criterion, such as frequency, value, or score

The package implements both in-mem and Redis backed solutions for the data structures. The
in-memory data structures are thread-safe.
*/
package gostatix

import (
	"container/heap"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

type heapElement struct {
	value     string
	frequency uint64
}

type minHeap []heapElement

func (h minHeap) Len() int {
	return len(h)
}

func (h minHeap) Less(i, j int) bool {
	return h[i].frequency < h[j].frequency
}

func (h minHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *minHeap) Push(x any) {
	*h = append(*h, x.(heapElement))
}

func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h minHeap) IndexOf(element string) int {
	for i := range h {
		if h[i].value == element {
			return i
		}
	}
	return -1
}

// In-memory TopK struct.
// _k_ is the number of top elements to track
// _errorRate_ is the acceptable error rate in topk estimation
// _accuracy_ is the delta in the error rate
// _sketch_ is the in-memory count-min sketch used to keep the estimated track of counts
// _heap_ is a min heap
type TopK struct {
	k         uint
	errorRate float64
	accuracy  float64
	sketch    *CountMinSketch
	heap      minHeap
}

// TopKElement is the struct used to return the results of the TopK
type TopKElement struct {
	element string
	count   uint64
}

// NewTopK creates new TopK
// _k_ is the number of top elements to track
// _errorRate_ is the acceptable error rate in topk estimation
// _accuracy_ is the delta in the error rate
func NewTopK(k uint, errorRate, accuracy float64) *TopK {
	sketch, _ := NewCountMinSketchFromEstimates(errorRate, accuracy)
	heap := &minHeap{}
	return &TopK{k, errorRate, accuracy, sketch, *heap}
}

// Insert puts the _data_ (byte slice) in the TopK data structure with _count_
// _data_ is the element to be inserted
// _count_ is the count of the element
func (t *TopK) Insert(data []byte, count uint64) {
	element := string(data)
	if count <= 0 {
		panic("count must be greater than zero")
	}
	sketch := t.sketch
	sketch.Update(data, count)
	frequency := sketch.Count(data)
	if uint(len(t.heap)) < t.k || frequency >= t.heap[0].frequency {
		index := t.heap.IndexOf(element)
		if index > -1 {
			heap.Remove(&t.heap, index)
		}
		heap.Push(&t.heap, heapElement{element, frequency})
		if uint(len(t.heap)) > t.k {
			heap.Pop(&t.heap)
		}
	}
}

// Values returns the top _k_ elements in the TopK data structure
func (t *TopK) Values() []TopKElement {
	var results []TopKElement
	for i := len(t.heap) - 1; i >= 0; i-- {
		results = append(results, TopKElement{t.heap[i].value, t.heap[i].frequency})
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
	return results
}

// internal type used to marshal/unmarshal heap elements
type heapElementJSON struct {
	Value     string `json:"v"`
	Frequency uint64 `json:"f"`
}

// internal type used to marshal/unmarshal TopK
type topKJSON struct {
	K         uint               `json:"k"`
	ErrorRate float64            `json:"er"`
	Accuracy  float64            `json:"a"`
	Sketch    countMinSketchJSON `json:"s"`
	Heap      []heapElementJSON  `json:"h"`
	HeapKey   string             `json:"hk"`
}

// Export JSON marshals the TopK and returns a byte slice containing the data
func (t *TopK) Export() ([]byte, error) {
	var sketch countMinSketchJSON
	sketch.AllSum = t.sketch.allSum
	sketch.Columns = t.sketch.columns
	sketch.Rows = t.sketch.rows
	sketch.Matrix = t.sketch.matrix
	var heap []heapElementJSON
	for i := range t.heap {
		heap = append(heap, heapElementJSON{Value: t.heap[i].value, Frequency: t.heap[i].frequency})
	}
	return json.Marshal(topKJSON{t.k, t.errorRate, t.accuracy, sketch, heap, ""})
}

// Import JSON unmarshals the _data_ into the TopK
func (t *TopK) Import(data []byte) error {
	var topk topKJSON
	err := json.Unmarshal(data, &topk)
	if err != nil {
		return fmt.Errorf("gostatix: error while unmarshalling data, error %v", err)
	}
	t.k = topk.K
	t.accuracy = topk.Accuracy
	t.errorRate = topk.ErrorRate
	var heap minHeap
	for i := range topk.Heap {
		heap = append(heap, heapElement{value: topk.Heap[i].Value, frequency: topk.Heap[i].Frequency})
	}
	t.heap = heap
	sketch, err := NewCountMinSketch(topk.Sketch.Rows, topk.Sketch.Columns)
	if err != nil {
		return fmt.Errorf("gostatix: error while unmarshalling data, error %v", err)
	}
	sketch.allSum = topk.Sketch.AllSum
	sketch.matrix = topk.Sketch.Matrix
	t.sketch = sketch
	return nil
}

// Equals checks if two TopK structures are equal
func (t *TopK) Equals(u *TopK) (bool, error) {
	if t.k != u.k {
		return false, fmt.Errorf("parameter k are not equal, %d and %d", t.k, u.k)
	}
	if t.accuracy != u.accuracy {
		return false, fmt.Errorf("parameter accuracy are not equal, %f and %f", t.accuracy, u.accuracy)
	}
	if t.errorRate != u.errorRate {
		return false, fmt.Errorf("parameter errorRate are not equal, %f and %f", t.errorRate, u.errorRate)
	}
	if !t.sketch.Equals(u.sketch) {
		return false, fmt.Errorf("sketches aren't equal")
	}
	for i := range t.heap {
		if t.heap[i] != u.heap[i] {
			return false, fmt.Errorf("heaps aren't equal")
		}
	}
	return true, nil
}

// WriteTo writes the TopK onto the specified _stream_ and returns the
// number of bytes written.
// It can be used to write to disk (using a file stream) or to network.
func (t *TopK) WriteTo(stream io.Writer) (int64, error) {
	err := binary.Write(stream, binary.BigEndian, uint64(t.k))
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, t.errorRate)
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, t.accuracy)
	if err != nil {
		return 0, err
	}
	numBytesSketch, err := t.sketch.WriteTo(stream)
	if err != nil {
		return 0, err
	}
	numBytesHeap := int64(0)
	for i := uint(0); i < t.k; i++ {
		element := t.heap[i]
		err := binary.Write(stream, binary.BigEndian, uint64(len(element.value)))
		if err != nil {
			return 0, err
		}
		bytesStr, err := stream.Write([]byte(element.value))
		if err != nil {
			return 0, err
		}
		err = binary.Write(stream, binary.BigEndian, element.frequency)
		if err != nil {
			return 0, err
		}
		numBytesHeap += int64(bytesStr + 2*binary.Size(uint64(0)))
	}
	return numBytesSketch + numBytesHeap + int64(3*binary.Size(uint64(0))), nil
}

// ReadFrom reads the TopK from the specified _stream_ and returns the
// number of bytes read.
// It can be used to read from disk (using a file stream) or from network.
func (t *TopK) ReadFrom(stream io.Reader) (int64, error) {
	var k uint64
	var errorRate, accuracy float64
	err := binary.Read(stream, binary.BigEndian, &k)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &errorRate)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &accuracy)
	if err != nil {
		return 0, err
	}
	sketch, _ := NewCountMinSketch(1, 1)
	numBytesSketch, err := sketch.ReadFrom(stream)
	if err != nil {
		return 0, err
	}
	numBytesHeap := int64(0)
	heap := &minHeap{}
	for i := uint64(0); i < k; i++ {
		var strLen, frequency uint64
		err := binary.Read(stream, binary.BigEndian, &strLen)
		if err != nil {
			return 0, err
		}
		b := make([]byte, strLen)
		_, err = io.ReadFull(stream, b)
		if err != nil {
			return 0, err
		}
		err = binary.Read(stream, binary.BigEndian, &frequency)
		if err != nil {
			return 0, err
		}
		*heap = append(*heap, heapElement{value: string(b), frequency: frequency})
	}
	t.k = uint(k)
	t.accuracy = accuracy
	t.errorRate = errorRate
	t.heap = *heap
	t.sketch = sketch
	return numBytesSketch + numBytesHeap + int64(3*binary.Size(uint64(0))), nil
}

package count

import (
	"container/heap"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

type HeapElement struct {
	value     string
	frequency uint64
}

type MinHeap []HeapElement

func (h MinHeap) Len() int {
	return len(h)
}

func (h MinHeap) Less(i, j int) bool {
	return h[i].frequency < h[j].frequency
}

func (h MinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *MinHeap) Push(x any) {
	*h = append(*h, x.(HeapElement))
}

func (h *MinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h MinHeap) IndexOf(element string) int {
	for i := range h {
		if h[i].value == element {
			return i
		}
	}
	return -1
}

type TopK struct {
	k         uint
	errorRate float64
	accuracy  float64
	sketch    *CountMinSketch
	heap      MinHeap
}

type TopKElement struct {
	element string
	count   uint64
}

func NewTopK(k uint, errorRate, accuracy float64) *TopK {
	sketch, _ := NewCountMinSketchFromEstimates(errorRate, accuracy)
	heap := &MinHeap{}
	return &TopK{k, errorRate, accuracy, sketch, *heap}
}

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
		heap.Push(&t.heap, HeapElement{element, frequency})
		if uint(len(t.heap)) > t.k {
			heap.Pop(&t.heap)
		}
	}
}

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

type heapElementJSON struct {
	Value     string `json:"v"`
	Frequency uint64 `json:"f"`
}

type topKJSON struct {
	K         uint               `json:"k"`
	ErrorRate float64            `json:"er"`
	Accuracy  float64            `json:"a"`
	Sketch    countMinSketchJSON `json:"s"`
	Heap      []heapElementJSON  `json:"h"`
	HeapKey   string             `json:"hk"`
}

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

func (t *TopK) Import(data []byte) error {
	var topk topKJSON
	err := json.Unmarshal(data, &topk)
	if err != nil {
		return fmt.Errorf("gostatix: error while unmarshalling data, error %v", err)
	}
	t.k = topk.K
	t.accuracy = topk.Accuracy
	t.errorRate = topk.ErrorRate
	var heap MinHeap
	for i := range topk.Heap {
		heap = append(heap, HeapElement{value: topk.Heap[i].Value, frequency: topk.Heap[i].Frequency})
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
	heap := &MinHeap{}
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
		*heap = append(*heap, HeapElement{value: string(b), frequency: frequency})
	}
	t.k = uint(k)
	t.accuracy = accuracy
	t.errorRate = errorRate
	t.heap = *heap
	t.sketch = sketch
	return numBytesSketch + numBytesHeap + int64(3*binary.Size(uint64(0))), nil
}

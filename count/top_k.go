package count

import "container/heap"

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
	sketch    CountMinSketch
	heap      MinHeap
}

type TopKElement struct {
	element string
	count   uint64
}

func NewTopK(k uint, errorRate, accuracy float64) *TopK {
	sketch, _ := NewCountMinSketchFromEstimates(errorRate, accuracy)
	heap := &MinHeap{}
	return &TopK{k, errorRate, accuracy, *sketch, *heap}
}

func (t *TopK) Insert(data []byte, count uint64) {
	element := string(data)
	if count <= 0 {
		panic("count must be greater than zero")
	}
	t.sketch.Update(data, count)
	frequency := t.sketch.Count(data)
	if uint(len(t.heap)) < t.k || frequency >= t.heap[0].frequency {
		index := t.heap.IndexOf(element)
		if index > -1 {
			heap.Remove(&t.heap, index)
		}
		heap.Push(&t.heap, &HeapElement{element, frequency})
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
	return results
}

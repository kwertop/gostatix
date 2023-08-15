package filters

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"math/rand"

	"github.com/kwertop/gostatix"
	"github.com/kwertop/gostatix/buckets"
)

type CuckooFilter struct {
	buckets []buckets.BucketMem
	*AbstractCuckooFilter
}

func NewCuckooFilter(size, bucketSize, fingerPrintLength uint64) *CuckooFilter {
	return NewCuckooFilterWithRetries(size, bucketSize, fingerPrintLength, 500)
}

func NewCuckooFilterWithRetries(size, bucketSize, fingerPrintLength, retries uint64) *CuckooFilter {
	filter := make([]buckets.BucketMem, size)
	for i := range filter {
		filter[i] = *buckets.NewBucketMem(bucketSize)
	}
	baseFilter := MakeAbstractCuckooFilter(size, bucketSize, fingerPrintLength, 0, retries)
	return &CuckooFilter{filter, baseFilter}
}

func NewCuckooFilterWithErrorRate(size, bucketSize, retries uint64, errorRate float64) *CuckooFilter {
	fingerPrintLength := gostatix.CalculateFingerPrintLength(size, errorRate)
	capacity := uint64(math.Ceil(float64(size) * 0.955 / float64(bucketSize)))
	return NewCuckooFilterWithRetries(capacity, bucketSize, fingerPrintLength, retries)
}

func (cuckooFilter *CuckooFilter) Insert(data []byte, destructive bool) bool {
	fingerPrint, fIndex, sIndex, _ := cuckooFilter.getPositions(data)
	if cuckooFilter.buckets[fIndex].IsFree() {
		cuckooFilter.buckets[fIndex].Add(fingerPrint)
	} else if cuckooFilter.buckets[sIndex].IsFree() {
		cuckooFilter.buckets[sIndex].Add(fingerPrint)
	} else {
		var index uint64
		if rand.Float32() < 0.5 {
			index = fIndex
		} else {
			index = sIndex
		}
		currFingerPrint := fingerPrint
		var items []Entry
		for i := uint64(0); i < cuckooFilter.retries; i++ {
			randIndex := uint64(math.Ceil(rand.Float64() * float64(cuckooFilter.buckets[index].Length()-1)))
			prevFingerPrint := cuckooFilter.buckets[index].At(randIndex)
			items = append(items, Entry{prevFingerPrint, index, randIndex})
			cuckooFilter.buckets[index].Set(randIndex, currFingerPrint)
			hash := getHash([]byte(prevFingerPrint))
			newIndex := (index ^ hash) % uint64(len(cuckooFilter.buckets))
			if cuckooFilter.buckets[newIndex].IsFree() {
				cuckooFilter.buckets[newIndex].Add(prevFingerPrint)
				cuckooFilter.length++
				return true
			}
		}
		if !destructive {
			for i := len(items) - 1; i >= 0; i-- {
				item := items[i]
				cuckooFilter.buckets[item.firstIndex].Set(item.secondIndex, item.fingerPrint)
			}
		}
		panic("cannot insert element, cuckoofilter is full")
	}
	cuckooFilter.length++
	return true
}

func (cuckooFilter *CuckooFilter) Lookup(data []byte) bool {
	fingerPrint, fIndex, sIndex, _ := cuckooFilter.getPositions(data)
	return cuckooFilter.buckets[fIndex].Lookup(fingerPrint) ||
		cuckooFilter.buckets[sIndex].Lookup(fingerPrint)
}

func (cuckooFilter *CuckooFilter) Remove(data []byte) bool {
	fingerPrint, fIndex, sIndex, _ := cuckooFilter.getPositions(data)
	if cuckooFilter.buckets[fIndex].Lookup(fingerPrint) {
		cuckooFilter.buckets[fIndex].Remove(fingerPrint)
		cuckooFilter.length--
		return true
	} else if cuckooFilter.buckets[sIndex].Lookup(fingerPrint) {
		cuckooFilter.buckets[sIndex].Remove(fingerPrint)
		cuckooFilter.length--
		return true
	} else {
		return false
	}
}

func (aFilter CuckooFilter) Equals(bFilter CuckooFilter) bool {
	count := 0
	result := true
	for result && count < len(aFilter.buckets) {
		bucket := aFilter.buckets[count]
		if !bFilter.buckets[count].Equals(bucket) {
			return false
		}
		count++
	}
	return true
}

type bucketMemJSON struct {
	Size     uint64   `json:"s"`
	Length   uint64   `json:"l"`
	Elements []string `json:"e"`
}

type cuckooFilterMemJSON struct {
	Size              uint64          `json:"s"`
	BucketSize        uint64          `json:"bs"`
	FingerPrintLength uint64          `json:"fpl"`
	Length            uint64          `json:"l"`
	Retries           uint64          `json:"r"`
	Buckets           []bucketMemJSON `json:"b"`
}

func (cuckooFilter CuckooFilter) Export() ([]byte, error) {
	bucketsJSON := make([]bucketMemJSON, cuckooFilter.size)
	for i := range cuckooFilter.buckets {
		bucket := cuckooFilter.buckets[i]
		bucketJSON := bucketMemJSON{bucket.Size(), bucket.Length(), bucket.Elements()}
		bucketsJSON[i] = bucketJSON
	}
	return json.Marshal(cuckooFilterMemJSON{
		cuckooFilter.size,
		cuckooFilter.bucketSize,
		cuckooFilter.fingerPrintLength,
		cuckooFilter.length,
		cuckooFilter.retries,
		bucketsJSON,
	})
}

func (cuckooFilter *CuckooFilter) Import(data []byte) error {
	var f cuckooFilterMemJSON
	err := json.Unmarshal(data, &f)
	if err != nil {
		return err
	}
	cuckooFilter.size = f.Size
	cuckooFilter.bucketSize = f.BucketSize
	cuckooFilter.fingerPrintLength = f.FingerPrintLength
	cuckooFilter.length = f.Length
	cuckooFilter.retries = f.Retries
	filters := make([]buckets.BucketMem, f.Size)
	for i := range f.Buckets {
		bucketJSON := f.Buckets[i]
		bucket := *buckets.NewBucketMem(f.BucketSize)
		for j := range bucketJSON.Elements {
			bucket.Add(bucketJSON.Elements[j])
		}
		filters[i] = bucket
	}
	cuckooFilter.buckets = filters
	return nil
}

func (cuckooFilter *CuckooFilter) WriteTo(stream io.Writer) (int64, error) {
	err := binary.Write(stream, binary.BigEndian, cuckooFilter.size)
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, cuckooFilter.bucketSize)
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, cuckooFilter.fingerPrintLength)
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, cuckooFilter.length)
	if err != nil {
		return 0, err
	}
	err = binary.Write(stream, binary.BigEndian, cuckooFilter.retries)
	if err != nil {
		return 0, err
	}
	numBytes := int64(0)
	for i := uint64(0); i < cuckooFilter.length; i++ {
		bytes, err := cuckooFilter.buckets[i].WriteTo(stream)
		if err != nil {
			return 0, err
		}
		numBytes += bytes
	}
	return numBytes + int64(5*binary.Size(uint64(0))), nil
}

func (cuckooFilter *CuckooFilter) ReadFrom(stream io.Reader) (int64, error) {
	var size, bucketSize, fingerPrintLength, length, retries uint64
	err := binary.Read(stream, binary.BigEndian, &size)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &bucketSize)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &fingerPrintLength)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &length)
	if err != nil {
		return 0, err
	}
	err = binary.Read(stream, binary.BigEndian, &retries)
	if err != nil {
		return 0, err
	}
	cuckooFilter.size = size
	cuckooFilter.bucketSize = bucketSize
	cuckooFilter.fingerPrintLength = fingerPrintLength
	cuckooFilter.length = length
	cuckooFilter.retries = retries
	cuckooFilter.buckets = make([]buckets.BucketMem, size)
	numBytes := int64(0)
	for i := uint64(0); i < cuckooFilter.length; i++ {
		bucket := &buckets.BucketMem{}
		bytes, err := bucket.ReadFrom(stream)
		if err != nil {
			return 0, err
		}
		numBytes += bytes
		cuckooFilter.buckets[i] = *bucket
	}
	return numBytes + int64(5*binary.Size(uint64(0))), nil
}

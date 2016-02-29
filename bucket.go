package bucket

import (
	"container/list"
	"sync"
)

// Bucket Concurrent storage container element
type Bucket interface {
	// Push Insert the elements to the bucket,return bucket length
	// If the exception occurs, it returns an error
	Push(v interface{}) (int, error)
	// Pop Pop an element
	// If the exception occurs, it returns an error
	Pop() (interface{}, error)
	// Len Get bucket length
	Len() int
	// ToSlice Get all the elements of the slice
	// If the exception occurs, it returns an error
	ToSlice() ([]interface{}, error)
	// Reset Empty bucket elements
	Reset()
	// Clone Get bucket duplicate
	Clone() Bucket
	// CloneAndReset Get bucket duplicate and empty elements
	CloneAndReset() Bucket
}

// NewListBucket Based on the list container implementation bucket
func NewListBucket() Bucket {
	return &lBucket{
		data: list.New(),
	}
}

// lBucket Based on the list container implementation bucket
type lBucket struct {
	sync.RWMutex
	data *list.List
}

func (b *lBucket) Push(v interface{}) (int, error) {
	b.Lock()
	defer b.Unlock()
	b.data.PushBack(v)
	count := b.data.Len()
	return count, nil
}

func (b *lBucket) Pop() (interface{}, error) {
	b.Lock()
	defer b.Unlock()
	ele := b.data.Front()
	if ele == nil || ele.Value == nil {
		return nil, nil
	}
	b.data.Remove(ele)
	return ele.Value, nil
}

func (b *lBucket) Len() int {
	b.RLock()
	defer b.RUnlock()
	return b.data.Len()
}

func (b *lBucket) ToSlice() ([]interface{}, error) {
	var data []interface{}
	b.RLock()
	defer b.RUnlock()
	for e := b.data.Front(); e != nil; e = e.Next() {
		data = append(data, e.Value)
	}
	return data, nil
}

func (b *lBucket) Reset() {
	b.Lock()
	defer b.Unlock()
	b.data = b.data.Init()
}

func (b *lBucket) Clone() Bucket {
	lData := list.New()
	b.RLock()
	defer b.RUnlock()
	lData.PushBackList(b.data)
	return &lBucket{data: lData}
}

func (b *lBucket) CloneAndReset() Bucket {
	lData := list.New()
	b.Lock()
	defer b.Unlock()
	lData.PushBackList(b.data)
	b.data = b.data.Init()
	return &lBucket{data: lData}
}

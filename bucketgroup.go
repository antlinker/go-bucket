package bucket

import (
	"errors"
	"sync"
	"sync/atomic"
)

const (
	_DefaultPoolsLen = 1 << 6
)

// BucketGroup Concurrent storage elements to the bucket group
// return to a specified number of a single bucket
type BucketGroup interface {
	// Open Open the bucket group waits to receive bucket
	// If already open,it returns an error
	Open() (<-chan Bucket, error)
	// Push Insert the elements to the bucket group
	// If the exception occurs, it returns an error
	Push(v interface{}) error
	// Len Get current use bucket length
	Len() int
	// Close Close bucket group,stop receive data
	// If the exception occurs, it returns an error
	Close() error
}

// NewBucketGroup Create instances of BucketGroup
// popBucketElementCount The number of pop-up bucket required elements
// bucketPoolsNum The number of buckets within the group
func NewBucketGroup(popBucketElementCount int, bucketPoolsNum ...int) BucketGroup {
	if popBucketElementCount == 0 {
		popBucketElementCount = _DefaultPoolsLen
	}
	bGroup := &bucketGroup{
		popBucketElementCount: popBucketElementCount,
		bucketPoolsLen:        _DefaultPoolsLen,
		popBucketIndex:        -1,
		chPopBucketIndex:      make(chan int32),
		chComplete:            make(chan struct{}),
	}
	if len(bucketPoolsNum) > 0 && bucketPoolsNum[0] > 0 {
		bGroup.bucketPoolsLen = int32(bucketPoolsNum[0])
	}
	bGroup.bucketPools = make([]Bucket, int(bGroup.bucketPoolsLen))
	for i := 0; i < int(bGroup.bucketPoolsLen); i++ {
		bGroup.bucketPools[i] = NewListBucket()
	}
	return bGroup
}

type bucketGroup struct {
	popBucketElementCount int
	isOpen                bool
	closeMutex            sync.RWMutex
	isClose               bool
	bucketPoolsLen        int32
	bucketPools           []Bucket
	currentBucketIndex    int32
	popBucketIndex        int32
	chPopBucketIndex      chan int32
	chComplete            chan struct{}
}

func (bg *bucketGroup) Open() (<-chan Bucket, error) {
	if bg.isOpen {
		return nil, errors.New("Bucket group already open!")
	}
	bg.isOpen = true
	chBucket := make(chan Bucket)
	go func() {
		for index := range bg.chPopBucketIndex {
			chBucket <- bg.bucketPools[int(index)].CloneAndReset()
		}
		close(chBucket)
		close(bg.chComplete)
	}()
	return chBucket, nil
}

func (bg *bucketGroup) Push(v interface{}) error {
	bg.closeMutex.RLock()
	isClose := bg.isClose
	bg.closeMutex.RUnlock()
	if isClose {
		return errors.New("Bucket group already close!")
	}
	cBucket := bg.bucketPools[int(bg.currentBucketIndex)]
	cCount, err := cBucket.Push(v)
	if err != nil {
		return err
	}
	if cCount >= bg.popBucketElementCount {
		if !atomic.CompareAndSwapInt32(&bg.currentBucketIndex, bg.bucketPoolsLen-1, 0) {
			atomic.AddInt32(&bg.currentBucketIndex, 1)
		}
		if !atomic.CompareAndSwapInt32(&bg.popBucketIndex, bg.bucketPoolsLen-1, 0) {
			atomic.AddInt32(&bg.popBucketIndex, 1)
		}
		bg.chPopBucketIndex <- bg.popBucketIndex
	}
	return nil
}

func (bg *bucketGroup) Len() int {
	cBucket := bg.bucketPools[int(bg.currentBucketIndex)]
	return cBucket.Len()
}

func (bg *bucketGroup) Close() error {
	bg.closeMutex.RLock()
	isClose := bg.isClose
	bg.closeMutex.RUnlock()
	if isClose {
		return errors.New("Bucket group already close!")
	}
	bg.closeMutex.Lock()
	bg.isClose = true
	bg.closeMutex.Unlock()
	cBucket := bg.bucketPools[int(bg.currentBucketIndex)]
	if cBucket.Len() > 0 {
		bg.chPopBucketIndex <- bg.currentBucketIndex
	}
	bg.chPopBucketIndex <- bg.currentBucketIndex + 1
	close(bg.chPopBucketIndex)
	<-bg.chComplete
	return nil
}

package bucket_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antlinker/go-bucket"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("BucketGroup Sync Test", func() {
	var (
		bGroup                bucket.BucketGroup
		wg                    sync.WaitGroup
		bucketCount           = 100
		bucketPoolsNum        = 10
		writeBucketGroupCount = 10
	)
	BeforeEach(func() {
		bGroup = bucket.NewBucketGroup(bucketCount, bucketPoolsNum)
		buckets, err := bGroup.Open()
		if err != nil {
			Fail("打开容器发生异常:" + err.Error())
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for bucket := range buckets {
				By("对比获取的获取大小")
				Expect(bucket.Len()).To(BeNumerically("==", bucketCount))
				bucketData, err := bucket.ToSlice()
				if err != nil {
					Fail("获取容器数据异常:" + err.Error())
					return
				}
				for i := 0; i < bucketCount; i++ {
					Expect(bucketData[i]).To(Equal(i + 1))
				}
			}
		}()
	})
	It("写入数据测试", func() {
		for i := 0; i < writeBucketGroupCount; i++ {
			for j := 0; j < bucketCount; j++ {
				err := bGroup.Push(j + 1)
				if err != nil {
					Fail("写入数据出现异常：" + err.Error())
					return
				}
				l := j + 1
				if l == bucketCount {
					l = 0
				}
				Expect(bGroup.Len()).To(BeNumerically("==", l))
			}
		}
	})
	AfterEach(func() {
		bGroup.Close()
		wg.Wait()
	})
})

var _ = Describe("BucketGroup Benchmark Test", func() {
	var (
		bGroup           bucket.BucketGroup
		wg               sync.WaitGroup
		bucketCount      = 100
		bucketPoolsNum   = 100
		writeBucketCount = 100000
		samplesCount     = 100
		writeNum         int64
		resultNum        int64
		startTime        time.Time
	)
	BeforeSuite(func() {
		bGroup = bucket.NewBucketGroup(bucketCount, bucketPoolsNum)
		buckets, err := bGroup.Open()
		if err != nil {
			Fail("打开容器发生异常:" + err.Error())
			return
		}
		wg.Add(1)
		startTime = time.Now()
		go func() {
			defer wg.Done()
			for bucket := range buckets {
				resultNum += int64(bucket.Len())
			}
			Expect(resultNum).To(Equal(writeNum))
		}()
	})
	Measure("并发写入测试", func(b Benchmarker) {
		pushRuntime := b.Time("push", func() {
			for i := 0; i < writeBucketCount; i++ {
				vNum := atomic.AddInt64(&writeNum, 1)
				err := bGroup.Push(vNum)
				if err != nil {
					Fail("写入数据出现异常：" + err.Error())
					return
				}
			}
		})
		Expect(pushRuntime.Seconds()).To(BeNumerically("<", 1), "写入数据超时")
	}, samplesCount)
	AfterSuite(func() {
		bGroup.Close()
		wg.Wait()
		fmt.Println("\n写入数据条数：", resultNum)
		fmt.Printf("总耗时：%.2fs\n", time.Now().Sub(startTime).Seconds())
	})
})

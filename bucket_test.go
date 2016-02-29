package bucket_test

import (
	"github.com/antlinker/go-bucket"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bucket Test", func() {
	var (
		lBucket bucket.Bucket
		count   = 100
	)
	BeforeEach(func() {
		lBucket = bucket.NewListBucket()
		By("初始化数据")
		for i := 0; i < count; i++ {
			n, err := lBucket.Push(i)
			if err != nil {
				Fail("写入数据时发生错误：" + err.Error())
				return
			}
			Expect(n).Should(Equal(i + 1))
		}
	})
	It("Len Test", func() {
		Expect(lBucket.Len()).Should(Equal(count))
	})
	It("Pop Test", func() {
		ele, err := lBucket.Pop()
		if err != nil {
			Fail("弹出元素发生错误：" + err.Error())
			return
		}
		Expect(ele).Should(Equal(0))
	})
	It("ToSlice Test", func() {
		vals, err := lBucket.ToSlice()
		if err != nil {
			Fail("ToSlice error:" + err.Error())
			return
		}
		for i := 0; i < len(vals); i++ {
			Expect(vals[i]).Should(Equal(i))
		}
	})
	It("Reset Test", func() {
		lBucket.Reset()
		Expect(lBucket.Len()).To(Equal(0))
	})
	It("Clone Test", func() {
		nVals := lBucket.Clone()
		Expect(nVals.Len()).To(Equal(count))
	})
	It("CloneAndReset Test", func() {
		nVals := lBucket.CloneAndReset()
		Expect(nVals.Len()).To(Equal(count))
		Expect(lBucket.Len()).To(Equal(0))
	})
})

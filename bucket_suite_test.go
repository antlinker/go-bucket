package bucket_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestConcurrencyMapBuckets(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Bucket Suite Test")
}

package keyfs_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestFlyfs(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Flyfs Suite")
}

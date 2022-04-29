package batch_test

import (
	"cesium/internal/batch"
	"cesium/kfs"
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type CounterOperation struct {
	count int
}

func (c *CounterOperation) Context() context.Context {
	return context.Background()
}

func (c *CounterOperation) FileKey() int {
	return 1
}

func (c *CounterOperation) Exec(f kfs.File[int]) {
	c.count++
}

func (c *CounterOperation) SendError(err error) {
	panic(err)
}

var _ = Describe("Batch", func() {
	Describe("OperationSet", func() {
		It("Should execute child operations sequentially", func() {
			op1 := &CounterOperation{}
			op2 := &CounterOperation{}
			opSet := batch.OperationSet[int, batch.Operation[int]]{op1, op2}
			var f kfs.File[int]
			opSet.Exec(f)
			Expect(opSet.FileKey()).To(Equal(1))
			Expect(opSet.Context()).To(Equal(context.Background()))
			Expect(func() { opSet.SendError(nil) }).ToNot(Panic())
			Expect(op1.count).To(Equal(1))
			Expect(op2.count).To(Equal(1))
		})
	})

})

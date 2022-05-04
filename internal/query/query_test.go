package query_test

import (
	"context"
	"github.com/arya-analytics/cesium/internal/operation"
	"github.com/arya-analytics/cesium/internal/query"
	"github.com/arya-analytics/cesium/shut"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type (
	fileKey          = int16
	simplePackage    = query.Package[fileKey, simpleQuery, operation.Operation[fileKey]]
	executionContext = query.ExecutionContext[fileKey, simpleQuery, operation.Operation[fileKey]]
)

const simpleKey query.OptKey = "simpleKey"

type simpleQuery struct {
	query.Query
	pkg *simplePackage
}

func (q simpleQuery) Exec(ctx context.Context) error {
	return q.pkg.Exec(ctx, q)
}

type simpleAssembler struct{}

func (s simpleAssembler) New(p *simplePackage) simpleQuery {
	return simpleQuery{pkg: p}
}

type simpleParser struct{}

func (s simpleParser) Parse(q simpleQuery) []operation.Operation[fileKey] {
	return []operation.Operation[fileKey]{}
}

type simpleExecutor struct{}

func (s simpleExecutor) Exec(ctx executionContext) error {
	return nil
}

var _ = Describe("Query", func() {
	Describe("Package", func() {
		It("Should execute the packaged components in the correct sequence", func() {
			s := shut.New()
			pkg := &simplePackage{
				Assembler: simpleAssembler{},
				Parser:    simpleParser{},
				Shutdown:  s,
				Executor:  simpleExecutor{},
			}
			err := pkg.New().Exec(context.Background())
			Expect(err).ToNot(HaveOccurred())
		})
	})
	Describe("Query", func() {
		Describe("Options", func() {
			It("Should get and set an option correctly", func() {
				q := query.New()
				q.Set(simpleKey, "simpleValue")
				Expect(q.Get(simpleKey)).To(Equal("simpleValue"))
			})
		})
	})
})

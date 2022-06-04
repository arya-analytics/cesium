package cesium_test

import _ "net/http/pprof"

import (
	"fmt"
	"github.com/arya-analytics/cesium"
	"github.com/arya-analytics/cesium/internal/testutil/seg"
	"github.com/arya-analytics/x/alamos"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"sync"
)

type createVars struct {
	nChannels int
	dataRate  cesium.DataRate
	dataType  cesium.DataType
}

type createConfig struct {
	next int
	vars []createVars
}

func (c *createConfig) Next() (createVars, error) {
	if c.next >= len(c.vars) {
		return createVars{}, io.EOF
	}
	v := c.vars[c.next]
	c.next++
	return v, nil
}

var progressiveCreate = []createVars{
	{
		nChannels: 1,
		dataRate:  5 * cesium.Hz,
		dataType:  cesium.Float64,
	},
	{
		nChannels: 2,
		dataRate:  12 * cesium.Hz,
		dataType:  cesium.Float64,
	},
	{
		nChannels: 3,
		dataRate:  20 * cesium.Hz,
		dataType:  cesium.Float64,
	},
	{
		nChannels: 4,
		dataRate:  30 * cesium.Hz,
		dataType:  cesium.Float64,
	},
	{
		nChannels: 5,
		dataRate:  100 * cesium.Hz,
		dataType:  cesium.Float64,
	},
	{
		nChannels: 500,
		dataRate:  1 * cesium.Hz,
		dataType:  cesium.Float64,
	},
	{
		nChannels: 100,
		dataRate:  20 * cesium.Hz,
		dataType:  cesium.Float64,
	},
}

var _ = FDescribe("Create", func() {
	var (
		db  cesium.DB
		log *zap.Logger
		exp alamos.Experiment
	)
	BeforeEach(func() {
		var err error
		log = zap.NewNop()
		exp = alamos.New("create_test")
		db, err = cesium.Open("./testdata",
			cesium.WithLogger(log),
			cesium.WithExperiment(exp),
		)
		Expect(err).ToNot(HaveOccurred())
	})
	AfterEach(func() {
		Expect(db.Close()).To(Succeed())
		rpt, err := exp.Report().JSON()
		Expect(err).ToNot(HaveOccurred())
		Expect(ioutil.WriteFile("create_test.json", rpt, 0644)).To(Succeed())
	})
	Describe("Simple", Ordered, func() {
		config := &createConfig{vars: progressiveCreate}
		p := alamos.NewParametrize[createVars](config)
		p.Template(func(i int, values createVars) {
			It(fmt.Sprintf("Should write data to %v channels in different goroutines correctly", values.nChannels), func() {
				chs, err := db.NewCreateChannel().
					WithRate(values.dataRate).
					WithType(values.dataType).
					ExecN(ctx, values.nChannels)
				Expect(err).ToNot(HaveOccurred())
				Expect(chs).To(HaveLen(values.nChannels))
				wg := &sync.WaitGroup{}
				wg.Add(values.nChannels)
				for _, ch := range chs {
					go func(ch cesium.Channel) {
						defer GinkgoRecover()
						req, res, err := db.NewCreate().WhereChannels(ch.Key).Stream(ctx)
						Expect(err).ToNot(HaveOccurred())
						stc := &seg.StreamCreate{
							Req:               req,
							Res:               res,
							SequentialFactory: seg.NewSequentialFactory(seg.RandFloat64, 10*cesium.Second, ch),
						}
						stc.CreateCRequestsOfN(100, 1)
						Expect(stc.CloseAndWait()).To(Succeed())
						wg.Done()
					}(ch)
				}
				wg.Wait()
			})
		})
		p.Construct()
	})
	Describe("Multi", func() {
		config := &createConfig{vars: progressiveCreate}
		p := alamos.NewParametrize[createVars](config)
		p.Template(func(i int, values createVars) {
			It(fmt.Sprintf("Should write to %v channels in a single goroutine corectly", values.nChannels), func() {
				chs, err := db.NewCreateChannel().
					WithRate(values.dataRate).
					WithType(values.dataType).
					ExecN(ctx, values.nChannels)
				Expect(err).ToNot(HaveOccurred())
				Expect(chs).To(HaveLen(values.nChannels))
				req, res, err := db.NewCreate().WhereChannels(cesium.ChannelKeys(chs)...).Stream(ctx)
				stc := &seg.StreamCreate{
					Req:               req,
					Res:               res,
					SequentialFactory: seg.NewSequentialFactory(seg.RandFloat64, 10*cesium.Second, chs...),
				}
				stc.CreateCRequestsOfN(1, 1)
				Expect(stc.CloseAndWait()).To(Succeed())
			})
		})
		p.Construct()
	})
})

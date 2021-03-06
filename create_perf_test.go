package cesium_test

import (
	"github.com/arya-analytics/cesium/testutil/seg"
	_ "net/http/pprof"
)

import (
	"fmt"
	"github.com/arya-analytics/cesium"
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
	dataType  cesium.Density
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
	{
		nChannels: 10,
		dataRate:  25 * cesium.KHz,
		dataType:  cesium.Float64,
	},
}

var _ = Describe("Create", func() {
	var (
		db      cesium.DB
		log     *zap.Logger
		exp     alamos.Experiment
		factory seg.DataFactory
	)
	BeforeEach(func() {
		var err error
		log = zap.NewNop()
		exp = alamos.New("create_test")
		db, err = cesium.Open("./testdata",
			cesium.WithLogger(log),
			cesium.WithExperiment(exp),
		)
		factory = &seg.RandomFloat64Factory{Cache: true}
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
			It(fmt.Sprintf("Should write data to %v channels in different goroutines"+
				" correctly", values.nChannels), func() {
				var (
					channels []cesium.Channel
				)
				for i := 0; i < values.nChannels; i++ {
					ch := cesium.Channel{
						DataRate: values.dataRate,
						DataType: values.dataType,
					}
					key, err := db.CreateChannel(ch)
					Expect(err).ToNot(HaveOccurred())
					ch.Key = key
					channels = append(channels, ch)
				}
				Expect(channels).To(HaveLen(values.nChannels))
				wg := &sync.WaitGroup{}
				wg.Add(values.nChannels)
				for _, ch := range channels {
					go func(ch cesium.Channel) {
						defer GinkgoRecover()
						req, res, err := db.NewCreate().WhereChannels(ch.Key).Stream(ctx)
						Expect(err).ToNot(HaveOccurred())
						stc := &seg.StreamCreate{
							Req:               req,
							Res:               res,
							SequentialFactory: seg.NewSequentialFactory(factory, 1*cesium.Second, ch),
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
				var (
					channels []cesium.Channel
					keys     []cesium.ChannelKey
				)
				for i := 0; i < values.nChannels; i++ {
					ch := cesium.Channel{
						DataRate: values.dataRate,
						DataType: values.dataType,
					}
					key, err := db.CreateChannel(ch)
					Expect(err).ToNot(HaveOccurred())
					channels = append(channels, ch)
					keys = append(keys, key)
				}
				Expect(channels).To(HaveLen(values.nChannels))
				req, res, err := db.NewCreate().WhereChannels(keys...).Stream(ctx)
				Expect(err).ToNot(HaveOccurred())
				stc := &seg.StreamCreate{
					Req: req,
					Res: res,
					SequentialFactory: seg.NewSequentialFactory(
						factory,
						10*cesium.Second,
						channels...,
					),
				}
				stc.CreateCRequestsOfN(1, 1)
				Expect(stc.CloseAndWait()).To(Succeed())
			})
		})
		p.Construct()
	})
})

package cesium_test

import (
	"cesium"
	"cesium/alamos"
	"cesium/internal/testutil/seg"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"io"
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
	//{
	//	nChannels: 1,
	//	dataRate:  5 * cesium.Hz,
	//	dataType:  cesium.Float64,
	//},
	//{
	//	nChannels: 2,
	//	dataRate:  12 * cesium.Hz,
	//	dataType:  cesium.Float64,
	//},
	//{
	//	nChannels: 3,
	//	dataRate:  20 * cesium.Hz,
	//	dataType:  cesium.Float64,
	//},
	//{
	//	nChannels: 4,
	//	dataRate:  30 * cesium.Hz,
	//	dataType:  cesium.Float64,
	//},
	{
		nChannels: 5,
		dataRate:  10000 * cesium.Hz,
		dataType:  cesium.Float64,
	},
	//{
	//	nChannels: 500,
	//	dataRate:  1 * cesium.Hz,
	//	dataType:  cesium.Float64,
	//},
	//{
	//	nChannels: 100,
	//	dataRate:  20 * cesium.Hz,
	//	dataType:  cesium.Float64,
	//},
}

var _ = Describe("Create", func() {
	var (
		db cesium.DB
		//log *zap.Logger
	)
	BeforeEach(func() {
		var err error
		//log, _ = zap.NewProduction()
		db, err = cesium.Open("./testdata", cesium.MemBacked())
		Expect(err).ToNot(HaveOccurred())
	})
	AfterEach(func() {
		Expect(db.Close()).To(Succeed())
	})
	Describe("Simple", func() {
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
						stc.CreateCRequestsOfN(1, 1)
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

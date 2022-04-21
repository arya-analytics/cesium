package cesium_test

import (
	"cesium"
	"cesium/alamos"
	"cesium/util/testutil/seg"
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

var _ = Describe("Create", func() {
	Describe("Create", func() {
		var (
			db  cesium.DB
			exp alamos.Experiment
		)
		BeforeEach(func() {
			exp = alamos.New("cesium_test.Create.Create")
			var err error
			db, err = cesium.Open("testdata", cesium.MemBacked(), cesium.WithExperiment(exp))
			Expect(err).ToNot(HaveOccurred())
		})
		AfterEach(func() {
			Expect(db.Close()).To(Succeed())
		})
		config := &createConfig{
			vars: []createVars{
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
					dataRate:  40 * cesium.Hz,
					dataType:  cesium.Float64,
				},
				{
					nChannels: 100,
					dataRate:  100 * cesium.Hz,
					dataType:  cesium.Float64,
				},
				{
					nChannels: 500,
					dataRate:  20 * cesium.Hz,
					dataType:  cesium.Float64,
				},
			},
		}
		p := alamos.NewParametrize[createVars](config)
		p.Template(func(i int, values createVars) {
			It(fmt.Sprintf("Should write data to %v channels correctly", values.nChannels), func() {
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
						req, res, err := db.NewCreate().WhereChannels(ch.PK).Stream(ctx)
						Expect(err).ToNot(HaveOccurred())
						stc := &seg.StreamCreate{
							Req:               req,
							Res:               res,
							SequentialFactory: seg.NewSequentialFactory(ch, seg.RandFloat64, 10*cesium.Second),
						}
						stc.CreateCRequestsOfN(10, 2)
						Expect(stc.CloseAndWait()).To(Succeed())
						wg.Done()
					}(ch)
				}
				wg.Wait()
			})
		})
		p.Construct()
	})
})

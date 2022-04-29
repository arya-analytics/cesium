package testutil

import (
	"cesium"
	"cesium/internal/testutil/seg"
	"context"
	log "github.com/sirupsen/logrus"
	"io"
	"time"
)

type DeviceFactory struct {
	Ctx context.Context
	DB  cesium.DB
}

func (f DeviceFactory) New(dt cesium.DataType, dr cesium.DataRate, fi cesium.TimeSpan) *Device {
	return &Device{
		Ctx:           f.Ctx,
		DB:            f.DB,
		DataType:      dt,
		DataRate:      dr,
		FlushInterval: fi,
	}
}

type Device struct {
	Ctx           context.Context
	DB            cesium.DB
	DataType      cesium.DataType
	DataRate      cesium.DataRate
	FlushInterval cesium.TimeSpan
	res           <-chan cesium.CreateResponse
	cancelFlush   context.CancelFunc
}

func (d *Device) createChannel() (cesium.Channel, error) {
	return d.DB.NewCreateChannel().
		WithRate(d.DataRate).
		WithType(d.DataType).
		Exec(d.Ctx)
}

func (d *Device) Start() error {
	c, err := d.createChannel()
	if err != nil {
		return err
	}
	return d.writeSegments(c)
}

func (d *Device) writeSegments(c cesium.Channel) error {
	req, res, err := d.DB.NewCreate().WhereChannels(c.Key).Stream(d.Ctx)
	if err != nil {
		return err

	}
	d.res = res
	ctx, cancel := context.WithCancel(d.Ctx)
	d.cancelFlush = cancel
	sc := &seg.StreamCreate{
		Req: req,
		Res: res,
		SequentialFactory: &seg.Seq{
			FirstTS: 0,
			PrevTS:  0,
			Factory: seg.DataTypeFactory(d.DataType),
			Channel: c,
			Span:    d.FlushInterval,
		},
	}
	go func() {
		t := time.NewTicker(d.FlushInterval.Duration())
		defer t.Stop()
		defer close(req)
		for {
			select {
			case <-ctx.Done():
				return
			case resV := <-res:
				if resV.Err != nil {
					log.Error(err)
				}
			case <-t.C:
				req <- cesium.CreateRequest{Segments: sc.NextN(1)}
			}
		}
	}()
	return nil

}

func (d *Device) Stop() error {
	d.cancelFlush()
	resV := <-d.res
	if resV.Err == io.EOF {
		return nil
	}
	return resV.Err

}

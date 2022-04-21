package cesium

import (
	"context"
	"io"
)

func createSync(ctx context.Context, c Create, segments *[]Segment) error {
	req, res, err := c.Stream(ctx)
	if err != nil {
		return err
	}
	req <- CreateRequest{Segments: *segments}
	close(req)
	return (<-res).Err
}

func retrieveSync(ctx context.Context, r Retrieve, segments *[]Segment) error {
	res, err := r.Stream(ctx)
	if err != nil {
		return err
	}
	for resV := range res {
		if resV.Err != nil {
			return resV.Err
		}
		if resV.Err == io.EOF {
			return nil
		}
		*segments = append(*segments, resV.Segments...)
	}
	return nil
}

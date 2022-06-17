package cesium_test

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
	"testing"
)

var ctx = context.Background()

func TestCaesium(t *testing.T) {
	log.SetLevel(log.InfoLevel)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Caesium Suite")
}

// |||||| CREATE ||||||
//
//// A simple query to write a segment of data to a channel.
//func ExampleDB_NewCreate_simple() {
//	ctx := context.Background()
//
//	// New the database.
//	db, err := cesium.Open("", cesium.MemBacked())
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Create a new channel.
//	ch, err := db.CreateChannel().
//		WithType(cesium.Float64).
//		WithRate(5 * cesium.Hz).
//		Exec(ctx)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Define a new segment.
//	seg := cesium.Segment{
//		ChannelKey: ch.Key,
//		Start:      cesium.Now(),
//		Data:       cesium.MarshalFloat64([]float64{1, 2, 3, 4, 5}),
//	}
//
//	// Write the segment.
//	if err := db.Sync(ctx, db.NewCreate().WhereChannels(ch.Key), &[]cesium.Segment{seg}); err != nil {
//		log.Fatal(err)
//	}
//
//	// Shutdown the database.
//	if err := db.Close(); err != nil {
//		log.Fatal(err)
//	}
//}
//
//// A query that writes multiple segmentKV of data through a channel.
//func ExampleDB_NewCreate_multiSegment() {
//	ctx := context.Background()
//
//	// New the database.
//	db, err := cesium.Open("", cesium.MemBacked())
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Create a new channel.
//	ch, err := db.CreateChannel().
//		WithType(cesium.Float64).
//		WithRate(5 * cesium.Hz).
//		Exec(ctx)
//
//	// New the query.
//	req, res, err := db.NewCreate().WhereChannels(ch.Key).Stream(ctx)
//
//	// Listen for errors.
//	wg := sync.WaitGroup{}
//	wg.Add(1)
//	go func() {
//		defer wg.Done()
//		for resV := range res {
//			// An eof error indicates the query has persisted all segmentKV t0 disk.
//			if resV.Error == io.EOF {
//				return
//			}
//			// Any other error indicates the query has failed.
//			log.Fatal(resV.Error)
//		}
//	}()
//
//	// Write 5 segmentKV.
//	const nSegments = 5
//	t0 := cesium.Now()
//	for i := 0; i < nSegments; i++ {
//		// Define the segment.
//		// It'stream important to notice that the start times do not overlap.
//		seg := cesium.Segment{
//			ChannelKey: ch.Key,
//			Start:      t0.Add(cesium.TimeSpan(i) * 5 * cesium.Second),
//			Data:       cesium.MarshalFloat64([]float64{1, 2, 3, 4, 5}),
//		}
//		// Write the segment as a create request.
//		req <- cesium.CreateRequest{Segments: []cesium.Segment{seg}}
//	}
//
//	// Shutdown the query and wait for it to complete.
//	close(req)
//	wg.Wait()
//
//	// Shutdown the database
//	if err := db.Close(); err != nil {
//		log.Fatal(err)
//	}
//}
//
//// A query that writes segmentKV for multiple channels.
//func ExampleDB_NewCreate_multiChannel() {
//	ctx := context.Background()
//
//	// New the database.
//	db, err := cesium.Open("", cesium.MemBacked())
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Create channels.
//	const nChannels = 5
//	channels, err := db.CreateChannel().
//		WithType(cesium.Float64).
//		WithRate(5*cesium.Hz).
//		ExecN(ctx, nChannels)
//	if err != nil {
//		log.Fatal(err)
//	}
//	var cPKs []int16
//	for _, ch := range channels {
//		cPKs = append(cPKs, ch.Key)
//	}
//
//	// New the query.
//	req, res, err := db.NewCreate().WhereChannels(cPKs...).Stream(ctx)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Listen for errors.
//	wg := sync.WaitGroup{}
//	wg.Add(1)
//	go func() {
//		defer wg.Done()
//		for resV := range res {
//			// An eof error indicates the query has persisted all segmentKV t0 disk.
//			if resV.Error == io.EOF {
//				return
//			}
//			// Any other error indicates the query has failed.
//			log.Fatal(resV.Error)
//		}
//	}()
//
//	// Write 5 segmentKV for each channel.
//	const nSegments = 5
//	t0 := cesium.Now()
//	for i := 0; i < nSegments; i++ {
//		// Define the segmentKV.
//		segs := make([]cesium.Segment, nChannels)
//		for j := 0; j < nChannels; j++ {
//			// It'stream important to notice that the start times do not overlap.
//			segs[j] = cesium.Segment{
//				ChannelKey: channels[j].Key,
//				Start:      t0.Add(cesium.TimeSpan(i) * 5 * cesium.Second),
//				Data:       cesium.MarshalFloat64([]float64{1, 2, 3, 4, 5}),
//			}
//		}
//		// Write the segmentKV as a create request.
//		req <- cesium.CreateRequest{Segments: segs}
//	}
//
//	// Shutdown the query and wait for it to complete.
//	close(req)
//	wg.Wait()
//
//	// Shutdown the database.
//	if err := db.Close(); err != nil {
//		log.Fatal(err)
//	}
//}
//
//// |||||| RETRIEVE ||||||
//
//func ExampleDB_NewRetrieve_simple() {
//	// First we need to write a segment.
//	ctx := context.Background()
//
//	// New the database.
//	db, err := cesium.Open("", cesium.MemBacked())
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Create a new channel.
//	ch, err := db.CreateChannel().
//		WithType(cesium.Float64).
//		WithRate(5 * cesium.Hz).
//		Exec(ctx)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Define a new segment.
//	seg := cesium.Segment{
//		ChannelKey: ch.Key,
//		Start:      cesium.Now(),
//		Data:       cesium.MarshalFloat64([]float64{1, 2, 3, 4, 5}),
//	}
//
//	// Write the segment.
//	if err := db.Sync(ctx, db.NewCreate().WhereChannels(ch.Key), &[]cesium.Segment{seg}); err != nil {
//		log.Fatal(err)
//	}
//
//	// New the retrieve query.
//	var resSegs []cesium.Segment
//	err = db.Sync(ctx, db.NewRetrieve().WhereChannels(ch.Key).WhereTimeRange(cesium.TimeRangeMax), &resSegs)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Print the number of segmentKV.
//	fmt.Println(len(resSegs))
//	// Output:
//	// 1
//
//	// Shutdown the database.
//	if err := db.Close(); err != nil {
//		log.Fatal(err)
//	}
//}

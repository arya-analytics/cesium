package cesium_test

import (
	"cesium"
	"context"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
	"io"
	"sync"
	"testing"
)

var ctx = context.Background()

func TestCaesium(t *testing.T) {
	log.SetLevel(log.WarnLevel)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Caesium Suite")
}

// |||||| CREATE ||||||

// A simple query to write a segment of data to a channel.
func ExampleDB_NewCreate_simple() {
	ctx := context.Background()

	// Open the database.
	db, err := cesium.Open("", cesium.MemBacked())
	if err != nil {
		log.Fatal(err)
	}

	// Create a new Channel.
	ch, err := db.NewCreateChannel().
		WithType(cesium.Float64).
		WithRate(5 * cesium.Hz).
		Exec(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Define a new segment.
	seg := cesium.Segment{
		ChannelPK: ch.PK,
		Start:     cesium.Now(),
		Data:      cesium.MarshalFloat64([]float64{1, 2, 3, 4, 5}),
	}

	// Write the segment.
	if err := db.Sync(ctx, db.NewCreate().WhereChannels(ch.PK), &[]cesium.Segment{seg}); err != nil {
		log.Fatal(err)
	}

	// Close the database.
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}

// A query that writes multiple segments of data through a channel.
func ExampleDB_NewCreate_multiSegment() {
	ctx := context.Background()

	// Open the database.
	db, err := cesium.Open("", cesium.MemBacked())
	if err != nil {
		log.Fatal(err)
	}

	// Create a new Channel.
	ch, err := db.NewCreateChannel().
		WithType(cesium.Float64).
		WithRate(5 * cesium.Hz).
		Exec(ctx)

	// Open the query.
	req, res, err := db.NewCreate().WhereChannels(ch.PK).Stream(ctx)

	// Listen for errors.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for resV := range res {
			// An eof error indicates the query has persisted all segments t0 disk.
			if resV.Err == io.EOF {
				return
			}
			// Any other error indicates the query has failed.
			log.Fatal(resV.Err)
		}
	}()

	// Write 5 segments.
	const nSegments = 5
	t0 := cesium.Now()
	for i := 0; i < nSegments; i++ {
		// Define the segment.
		// It's important to notice that the start times do not overlap.
		seg := cesium.Segment{
			ChannelPK: ch.PK,
			Start:     t0.Add(cesium.TimeSpan(i) * 5 * cesium.Second),
			Data:      cesium.MarshalFloat64([]float64{1, 2, 3, 4, 5}),
		}
		// Write the segment as a create request.
		req <- cesium.CreateRequest{Segments: []cesium.Segment{seg}}
	}

	// Close the query and wait for it to complete.
	close(req)
	wg.Wait()

	// Close the database
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}

// A query that writes segments for multiple channels.
func ExampleDB_NewCreate_multiChannel() {
	ctx := context.Background()

	// Open the database.
	db, err := cesium.Open("", cesium.MemBacked())
	if err != nil {
		log.Fatal(err)
	}

	// Create channels.
	const nChannels = 5
	channels, err := db.NewCreateChannel().
		WithType(cesium.Float64).
		WithRate(5*cesium.Hz).
		ExecN(ctx, nChannels)
	if err != nil {
		log.Fatal(err)
	}
	var cPKs []cesium.PK
	for _, ch := range channels {
		cPKs = append(cPKs, ch.PK)
	}

	// Open the query.
	req, res, err := db.NewCreate().WhereChannels(cPKs...).Stream(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Listen for errors.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for resV := range res {
			// An eof error indicates the query has persisted all segments t0 disk.
			if resV.Err == io.EOF {
				return
			}
			// Any other error indicates the query has failed.
			log.Fatal(resV.Err)
		}
	}()

	// Write 5 segments for each channel.
	const nSegments = 5
	t0 := cesium.Now()
	for i := 0; i < nSegments; i++ {
		// Define the segments.
		segs := make([]cesium.Segment, nChannels)
		for j := 0; j < nChannels; j++ {
			// It's important to notice that the start times do not overlap.
			segs[j] = cesium.Segment{
				ChannelPK: channels[j].PK,
				Start:     t0.Add(cesium.TimeSpan(i) * 5 * cesium.Second),
				Data:      cesium.MarshalFloat64([]float64{1, 2, 3, 4, 5}),
			}
		}
		// Write the segments as a create request.
		req <- cesium.CreateRequest{Segments: segs}
	}

	// Close the query and wait for it to complete.
	close(req)
	wg.Wait()

	// Close the database.
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}

// |||||| RETRIEVE ||||||

func ExampleDB_NewRetrieve_simple() {
	// First we need to write a segment.
	ctx := context.Background()

	// Open the database.
	db, err := cesium.Open("", cesium.MemBacked())
	if err != nil {
		log.Fatal(err)
	}

	// Create a new Channel.
	ch, err := db.NewCreateChannel().
		WithType(cesium.Float64).
		WithRate(5 * cesium.Hz).
		Exec(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Define a new segment.
	seg := cesium.Segment{
		ChannelPK: ch.PK,
		Start:     cesium.Now(),
		Data:      cesium.MarshalFloat64([]float64{1, 2, 3, 4, 5}),
	}

	// Write the segment.
	if err := db.Sync(ctx, db.NewCreate().WhereChannels(ch.PK), &[]cesium.Segment{seg}); err != nil {
		log.Fatal(err)
	}

	// Open the retrieve query.
	var resSegs []cesium.Segment
	err = db.Sync(ctx, db.NewRetrieve().WhereChannels(ch.PK).WhereTimeRange(cesium.TimeRangeMax), &resSegs)
	if err != nil {
		log.Fatal(err)
	}

	// Print the number of segments.
	fmt.Println(len(resSegs))
	// Output:
	// 1

	// Close the database.
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}

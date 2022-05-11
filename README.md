<br />
<p align="center">
    <a href="https://aryaanalytics.com/">
        <img src="docs/media/icon-full-title-white.png" width="70%"/>
    </a>
</p>

# Cesium

Cesium extends CockroachDB's [Pebble](https://github.com/cockroachdb/pebble) to provide fast storage for time-series data. 

## Use Case

Cesium is tailored towards a specific class of time-series data:

1. Regular - samples are taken at specific, known intervals. Cesium will not work well with data that arrives at unpredictable 
intervals. 
2. High Speed - cesium is happiest at sample rates between 10 Hz and 1 MHz. Although it can work with data at any rate,
it will be far slower than other storage engines for low sample rates.

## Concepts

### Channels

A channel (`cesium.Channel`) is a time-ordered collection of samples. It's best to approach them as a device (whether physical or virtual)
that emits values with the following properties:

1. Time-ordered
2. Time-unique - no two values are emitted at the same time.
3. Constant size - all emitted values have the same amount of bytes.
4. Regular - all values are emitted at a constant interval.

These values are known as samples. Samples can be measurements from a sensor, events from a stream, metrics from a database, 
or images from a camera just to name a few.

### Segments

A segment (`cesium.Segment`) is a contiguous run of samples (between 1 b and 2.5 Mb). Cesium stores all values in a 
segment sequentially on disk, so it's naturally best to write large segments over small segments. 

This obviously has implications in terms of durability, as smaller segments will prevent data loss in the event of a
failure. It's up to you to weigh the performance vs. durability risk.

## Production Readiness

## Installation

```bash
go get github.com/arya-analytics/cesium
```

## Getting Started
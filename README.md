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
2. High Speed - cesium is happiest at sample rates between 10 Hz and 1 MHz. Although it can work with data at any sample rate,
it will be far slower than other storage engines for low sample rates.


## Concepts

### Channels

### Segments

## Production Readiness

## Installation

```bash
go get github.com/arya-analytics/cesium
```

## Getting Started
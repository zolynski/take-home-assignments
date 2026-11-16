package main

import (
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const name = "dash0.com/otlp-metric-store-backend"

var (
	meter  = otel.Meter(name)
	logger = otelslog.NewLogger(name)

	exportRequestsCounter    metric.Int64Counter
	dataPointsReceived       metric.Int64Counter
	dataPointsStored         metric.Int64Counter
	dataPointsRejected       metric.Int64Counter
	seriesCacheLookups       metric.Int64Counter
	seriesWritten            metric.Int64Counter
	clickhouseInsertDuration metric.Float64Histogram
	clickhouseInsertRows     metric.Int64Histogram
	clickhouseInsertErrors   metric.Int64Counter
)

func init() {
	exportRequestsCounter = must(meter.Int64Counter("com.dash0.homeexercise.export.requests",
		metric.WithDescription("The number of OTLP metrics export requests received"),
		metric.WithUnit("{request}")))
	dataPointsReceived = must(meter.Int64Counter("com.dash0.homeexercise.datapoints.received",
		metric.WithDescription("The number of data points received, by metric type"),
		metric.WithUnit("{datapoint}")))
	dataPointsStored = must(meter.Int64Counter("com.dash0.homeexercise.datapoints.stored",
		metric.WithDescription("The number of data points written to ClickHouse, by metric type"),
		metric.WithUnit("{datapoint}")))
	dataPointsRejected = must(meter.Int64Counter("com.dash0.homeexercise.datapoints.rejected",
		metric.WithDescription("The number of data points rejected as invalid or unsupported, by metric type and reason"),
		metric.WithUnit("{datapoint}")))
	seriesCacheLookups = must(meter.Int64Counter("com.dash0.homeexercise.series.cache.lookups",
		metric.WithDescription("Series cache lookups, by result; a miss means the series row is written for that day"),
		metric.WithUnit("{lookup}")))
	seriesWritten = must(meter.Int64Counter("com.dash0.homeexercise.series.written",
		metric.WithDescription("The number of series rows written to ClickHouse"),
		metric.WithUnit("{series}")))
	clickhouseInsertDuration = must(meter.Float64Histogram("com.dash0.homeexercise.clickhouse.insert.duration",
		metric.WithDescription("Duration of ClickHouse batch inserts, by table"),
		metric.WithUnit("s")))
	clickhouseInsertRows = must(meter.Int64Histogram("com.dash0.homeexercise.clickhouse.insert.rows",
		metric.WithDescription("Rows per ClickHouse batch insert, by table; small batches under load mean exports are not being combined"),
		metric.WithUnit("{row}"),
		metric.WithExplicitBucketBoundaries(1, 10, 100, 1_000, 10_000, 100_000, 1_000_000)))
	clickhouseInsertErrors = must(meter.Int64Counter("com.dash0.homeexercise.clickhouse.insert.errors",
		metric.WithDescription("The number of failed ClickHouse batch inserts, by table"),
		metric.WithUnit("{insert}")))
}

func must[T any](instrument T, err error) T {
	if err != nil {
		panic(err)
	}
	return instrument
}

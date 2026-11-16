package main

const (
	seriesTable = "otel_metrics_series"
	gaugeTable  = "otel_metrics_gauge"
	sumTable    = "otel_metrics_sum"
)

// The series table holds one row per series per day, so a time-bounded query prunes it by partition
// just like the data point tables. ReplacingMergeTree collapses rows re-sent within a day; ServiceName
// and MetricName are derived from SeriesId, so leading the key with them doesn't change deduplication.
// Day is in the key because SELECT ... FINAL deduplicates across partitions, not only within them.
const createSeriesTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_series (
    SeriesId UInt128,
    Day Date,
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl String CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl String CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit String CODEC(ZSTD(1)),
    MetricType LowCardinality(String) CODEC(ZSTD(1)),
    AggregationTemporality Int32 CODEC(ZSTD(1)),
    IsMonotonic Bool CODEC(ZSTD(1)),
    Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),

    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE ReplacingMergeTree()
PARTITION BY Day
ORDER BY (ServiceName, MetricName, SeriesId, Day)
SETTINGS index_granularity = 8192;
`

// Gauge and sum data points share this shape. Timestamps are UTC so toDate() partitions on the same day boundary the service uses for Day,
// whatever the server's timezone. The hour bucket leads the key so time-only queries skip granules
// within a day while points of one series stay together inside each hour.
// A series has one data point per timestamp, so ReplacingMergeTree collapses what a retrying exporter
// sends twice; until that merge runs, only SELECT ... FINAL is free of those duplicates.
// Value is NaN for a data point flagged as having no recorded value.
const createDataPointTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
    SeriesId UInt128,
    StartTimeUnix DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1)),
    TimeUnix DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1)),
    Value Float64 CODEC(ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1))
) ENGINE ReplacingMergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (toStartOfHour(TimeUnix), SeriesId, TimeUnix)
SETTINGS index_granularity = 8192;
`

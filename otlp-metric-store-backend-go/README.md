# OTLP Metric Storage (Go)

## Introduction
This take-home assignment is designed to give you an opportunity to demonstrate your skills and experience in
building a small backend application. We expect you to spend 3-4 hours on this assignment (using AI coding agents).
If you find yourself spending more time than that, please stop and submit what you have. We are not looking for a
complete solution, but rather a demonstration of your skills and experience.

To submit your solution, please create a public GitHub repository and send us the link. Please include a `README.md` file
with instructions on how to run your application.

## Overview
The goal of this assignment is to build a simple backend application that receives [metric datapoints](https://opentelemetry.io/docs/concepts/signals/metrics/)
on a gRPC endpoint and processes them, before storing in ClickHouse.
Current state is that we have a gRPC endpoint for receiving metrics, and Gauge and Sum type get correctly converted to
records and inserted into ClickHouse. This is tested with both unit- and integration-tests.

What we're looking for is to extract meta-data about the metrics into a separate table, which will then act as a 'lookup'
table, and that actual data-points just get stored as value + timestamp and with a reference to the lookup table.

Think about and keep in mind the following things:
- How to do the reference between tables?
- How to efficiently store the meta-data in ClickHouse?
- All data should be stored in such a way that full table scans are never needed, under the assumption data always gets queried for a specific time-frame
- Other than time-frame, there are no other mandatory filters for querying
- While you can assume cardinality of the metrics is 'low', e.g. Resources (Attributes) are likely to change over time 

Your solution should take into account high throughput, both in number of messages and the number of metrics / data-points per message.

Feel free to use the existing scaffoling in this folder. Of course, you can also change anything else as you see fit.

## Technology Constraints
- Your Go program should compile using standard Go SDK, and be compatible with Go 1.26.
- Use any additional libraries you want and need.

## Notes
- As this assignment is for the role of a Staff / Senior Product Engineer, we expect you to pay some attention to maintainability and operability of the solution. For example:
  - Consistent terminology usage
  - Validation of the behaviour
  - Include signals / events to help in debugging
- Assume that this application will be deployed to production. Build it accordingly.

## Solution

### Terminology
- **Series**: everything that describes a data point apart from its timestamps, value and flags — resource,
  scope, metric (name, description, unit, type, temporality, monotonicity) and data point attributes.
- **Series ID**: a 128-bit XXH3 hash of a canonical, type-tagged encoding of the series. Attribute order
  does not matter; any change to an attribute, including a resource attribute, makes a new series.
- **Data point**: a series ID with a start time, time, value and flags.

### Tables

| Table | Holds | Partition | Sort key |
|---|---|---|---|
| `otel_metrics_series` | One row per series per UTC day it has data points | `Day` | `ServiceName, MetricName, SeriesId, Day` |
| `otel_metrics_gauge`, `otel_metrics_sum` | `SeriesId, StartTimeUnix, TimeUnix, Value, Flags` | `toDate(TimeUnix)` | `toStartOfHour(TimeUnix), SeriesId, TimeUnix` |

- **The reference is the series ID.** It is computed from the content, so any number of instances agree on it
  without coordinating, and nothing has to be looked up before a data point can be written.
- **No table is ever scanned in full for a time-bounded query.** All three are partitioned by day, so a query
  reads only the days in its window; within a day, the data point tables' leading hour bucket lets a query
  filtered only by time skip the hours outside it. The integration tests run the queries below with
  ClickHouse's `force_index_by_date` and `force_primary_key`, which make it refuse a query that would not prune.
- **Series rows repeat per day** instead of being written once, so the series table can be pruned by time
  like the others, and so a series' metadata as of a given day is always next to its data points.
  Cardinality is low, so this costs little.
- **All tables are `ReplacingMergeTree`.** A series row written twice in a day (cache misses, restarts,
  several instances) and a data point an exporter retries collapse into one; read with `FINAL` to see them
  collapsed before ClickHouse merges.
- Histogram, exponential histogram and summary metrics are not stored.

### Write path
1. `Export` maps the request into series rows and data point rows, rejecting data points without a value,
   without a timestamp, or with one outside `-maxDataPointAge`/`-maxDataPointAhead` (which also bounds how
   many day partitions one insert can create). Rejections are reported in OTLP `partial_success`.
2. An LRU cache of (series, day) pairs drops series rows already written today, so in steady state almost
   no series rows are written.
3. New series rows are inserted first, so a stored data point never references a series whose insert failed;
   then gauge and sum data points are inserted concurrently.
4. Inserts from concurrent exports are combined per table (`-insertBatchMaxRows`, `-insertBatchMaxDelay`),
   because ClickHouse creates a part per insert. An export is acknowledged only once its rows are stored;
   if an insert fails it returns `UNAVAILABLE`, which exporters retry and which the tables make safe to retry.

### Querying
Gauge values of one metric within a time window, with their series' attributes:
```sql
SELECT s.ServiceName, s.ResourceAttributes, s.Attributes, p.TimeUnix, p.Value
FROM otel_metrics_gauge AS p FINAL
INNER JOIN (
    SELECT SeriesId, ServiceName, ResourceAttributes, Attributes
    FROM otel_metrics_series FINAL
    WHERE Day BETWEEN toDate({from:DateTime64(9)}) AND toDate({to:DateTime64(9)})
      AND MetricName = 'cpu.utilization'
    LIMIT 1 BY SeriesId
) AS s ON p.SeriesId = s.SeriesId
WHERE p.TimeUnix BETWEEN {from:DateTime64(9)} AND {to:DateTime64(9)}
ORDER BY p.TimeUnix
```
The series side must be filtered by the same days as the data points; `LIMIT 1 BY SeriesId` keeps one of a
series' per-day rows. A staleness marker (no recorded value) is stored with `Value = NaN`.

### Operability
- **Metrics** (`com.dash0.homeexercise.*`): export requests; data points received, stored and rejected, by
  metric type and rejection reason; series cache hits and misses; series rows written; ClickHouse insert
  duration, rows per insert and failed inserts, by table.
- **Health**: the standard gRPC health service, which reports `NOT_SERVING` once shutdown starts.
- **Shutdown**: on SIGTERM, in-flight exports finish and the last batches are inserted before exit.

### Throughput
`go test -run '^$' -bench MapMetrics .` maps a 10,000 data point request at about 2 million data points per
second on one core of an Apple M3 Pro. Mapping builds a row for every series in a request before the
cache is consulted; skipping that for cached series would cut most of its allocations.

### Not covered
- TLS for clients and for ClickHouse.
- Retention (TTL) and schema migrations: tables are created if missing and never altered.

## Usage

Build the application:
```shell
go build ./...
```

Start ClickHouse:
```shell
docker run -d --name clickhouse -p 9000:9000 \
  -e CLICKHOUSE_USER=default -e CLICKHOUSE_PASSWORD=secret \
  clickhouse/clickhouse-server:26.2
```

Run the application (creates its tables on startup):
```shell
CLICKHOUSE_PASSWORD=secret go run . -clickhouseAddr localhost:9000
```

| Flag | Default | |
|---|---|---|
| `-listenAddr` | `localhost:4317` | OTLP gRPC listen address |
| `-maxReceiveMessageSize` | `16777216` | Max gRPC message size in bytes |
| `-clickhouseAddr` | `localhost:9000` | ClickHouse native protocol address |
| `-clickhouseDatabase` | `default` | ClickHouse database |
| `-clickhouseUser` | `default` | ClickHouse user; the password comes from `CLICKHOUSE_PASSWORD` |
| `-seriesCacheSize` | `1000000` | (series, day) pairs remembered as already written |
| `-maxDataPointAge` | `168h` | Data points older than this are rejected |
| `-maxDataPointAhead` | `10m` | Data points further in the future than this are rejected |
| `-insertBatchMaxRows` | `100000` | Rows of concurrent exports combined into one insert before it is sent |
| `-insertBatchMaxDelay` | `200ms` | The longest an insert waits for more rows |

Run unit tests:
```shell
go test ./...
```

Run integration tests (needs Docker; starts ClickHouse with testcontainers):
```shell
go test -tags integration ./...
```

## References

- [OpenTelemetry Metrics](https://opentelemetry.io/docs/concepts/signals/metrics/)
- [OpenTelemetry Protocol (OTLP)](https://github.com/open-telemetry/opentelemetry-proto)

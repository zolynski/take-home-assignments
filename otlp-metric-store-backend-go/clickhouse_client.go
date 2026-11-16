package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// SeriesRow describes one series on one UTC day.
type SeriesRow struct {
	SeriesID               SeriesID
	Day                    time.Time
	ResourceAttributes     map[string]string
	ResourceSchemaUrl      string
	ScopeName              string
	ScopeVersion           string
	ScopeAttributes        map[string]string
	ScopeDroppedAttrCount  uint32
	ScopeSchemaUrl         string
	ServiceName            string
	MetricName             string
	MetricDescription      string
	MetricUnit             string
	MetricType             string
	AggregationTemporality int32
	IsMonotonic            bool
	Attributes             map[string]string
}

// DataPointRow is a single gauge or sum data point referencing its series.
type DataPointRow struct {
	SeriesID      SeriesID
	StartTimeUnix time.Time
	TimeUnix      time.Time
	Value         float64
	Flags         uint32
}

// MetricsStore persists series and data points.
type MetricsStore interface {
	InsertSeries(ctx context.Context, rows []SeriesRow) error
	InsertGauge(ctx context.Context, rows []DataPointRow) error
	InsertSum(ctx context.Context, rows []DataPointRow) error
}

// ClickHouseMetricsStore implements MetricsStore using a ClickHouse connection.
type ClickHouseMetricsStore struct {
	conn driver.Conn
}

// NewClickHouseMetricsStore creates a new ClickHouseMetricsStore connected to the given address.
func NewClickHouseMetricsStore(ctx context.Context, addr string, database string, username string, password string) (*ClickHouseMetricsStore, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("pinging clickhouse: %w", err)
	}
	return &ClickHouseMetricsStore{conn: conn}, nil
}

// CreateTables creates any missing tables.
func (s *ClickHouseMetricsStore) CreateTables(ctx context.Context) error {
	for _, ddl := range []string{
		createSeriesTableSQL,
		fmt.Sprintf(createDataPointTableSQL, gaugeTable),
		fmt.Sprintf(createDataPointTableSQL, sumTable),
	} {
		if err := s.conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("creating table: %w", err)
		}
	}
	return nil
}

// InsertSeries batch-inserts series rows into otel_metrics_series.
func (s *ClickHouseMetricsStore) InsertSeries(ctx context.Context, rows []SeriesRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+seriesTable)
	if err != nil {
		return fmt.Errorf("preparing series batch: %w", err)
	}
	defer batch.Close()
	for _, r := range rows {
		if err := batch.Append(
			r.SeriesID.BigInt(),
			r.Day,
			r.ResourceAttributes,
			r.ResourceSchemaUrl,
			r.ScopeName,
			r.ScopeVersion,
			r.ScopeAttributes,
			r.ScopeDroppedAttrCount,
			r.ScopeSchemaUrl,
			r.ServiceName,
			r.MetricName,
			r.MetricDescription,
			r.MetricUnit,
			r.MetricType,
			r.AggregationTemporality,
			r.IsMonotonic,
			r.Attributes,
		); err != nil {
			return fmt.Errorf("appending series row: %w", err)
		}
	}
	return batch.Send()
}

// InsertGauge batch-inserts data points into otel_metrics_gauge.
func (s *ClickHouseMetricsStore) InsertGauge(ctx context.Context, rows []DataPointRow) error {
	return s.insertDataPoints(ctx, gaugeTable, rows)
}

// InsertSum batch-inserts data points into otel_metrics_sum.
func (s *ClickHouseMetricsStore) InsertSum(ctx context.Context, rows []DataPointRow) error {
	return s.insertDataPoints(ctx, sumTable, rows)
}

func (s *ClickHouseMetricsStore) insertDataPoints(ctx context.Context, table string, rows []DataPointRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+table)
	if err != nil {
		return fmt.Errorf("preparing %s batch: %w", table, err)
	}
	defer batch.Close()
	for _, r := range rows {
		if err := batch.Append(
			r.SeriesID.BigInt(),
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
			r.Flags,
		); err != nil {
			return fmt.Errorf("appending %s row: %w", table, err)
		}
	}
	return batch.Send()
}

// Close closes the underlying ClickHouse connection.
func (s *ClickHouseMetricsStore) Close() error {
	return s.conn.Close()
}

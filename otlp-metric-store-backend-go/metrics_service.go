package main

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// One INSERT may touch at most this many day partitions: ClickHouse's default max_partitions_per_insert_block.
const maxDaysPerInsert = 100

type serverConfig struct {
	// SeriesCacheSize is the number of (series, day) pairs remembered as already written.
	SeriesCacheSize int
	// MaxDataPointAge and MaxDataPointAhead bound how far before and after Now a data point's timestamp may lie.
	MaxDataPointAge   time.Duration
	MaxDataPointAhead time.Duration
	// Now defaults to time.Now.
	Now func() time.Time
}

type dash0MetricsServiceServer struct {
	store MetricsStore
	cfg   serverConfig
	// writtenSeries remembers series rows already stored, so each is written about once per day per instance.
	writtenSeries *lru.Cache[seriesDay, struct{}]

	colmetricspb.UnimplementedMetricsServiceServer
}

func newServer(store MetricsStore, cfg serverConfig) (colmetricspb.MetricsServiceServer, error) {
	if cfg.MaxDataPointAge <= 0 || cfg.MaxDataPointAhead < 0 {
		return nil, fmt.Errorf("maxDataPointAge must be positive and maxDataPointAhead must not be negative")
	}
	// A window of n whole days can touch n+2 day partitions.
	maxWindow := (maxDaysPerInsert - 2) * 24 * time.Hour
	if cfg.MaxDataPointAge+cfg.MaxDataPointAhead > maxWindow {
		return nil, fmt.Errorf("maxDataPointAge plus maxDataPointAhead must not exceed %s, so one insert stays within %d day partitions", maxWindow, maxDaysPerInsert)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	cache, err := lru.New[seriesDay, struct{}](cfg.SeriesCacheSize)
	if err != nil {
		return nil, fmt.Errorf("creating series cache: %w", err)
	}
	return &dash0MetricsServiceServer{store: store, cfg: cfg, writtenSeries: cache}, nil
}

func (m *dash0MetricsServiceServer) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	slog.DebugContext(ctx, "Received ExportMetricsServiceRequest")
	exportRequestsCounter.Add(ctx, 1)

	now := m.cfg.Now()
	mapped := MapMetrics(request.GetResourceMetrics(), TimeWindow{
		Oldest: now.Add(-m.cfg.MaxDataPointAge),
		Newest: now.Add(m.cfg.MaxDataPointAhead),
	})
	m.recordReceived(ctx, mapped)

	var newSeries []SeriesRow
	for _, s := range mapped.Series {
		if m.writtenSeries.Contains(seriesDay{ID: s.SeriesID, Day: s.Day.Unix()}) {
			seriesCacheLookups.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "hit")))
			continue
		}
		seriesCacheLookups.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "miss")))
		newSeries = append(newSeries, s)
	}

	// Series go first, so a stored data point never references a series whose insert failed.
	if len(newSeries) > 0 {
		if err := m.store.InsertSeries(ctx, newSeries); err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		for _, s := range newSeries {
			m.writtenSeries.Add(seriesDay{ID: s.SeriesID, Day: s.Day.Unix()}, struct{}{})
		}
		seriesWritten.Add(ctx, int64(len(newSeries)))
	}

	g, gctx := errgroup.WithContext(ctx)
	if len(mapped.Gauge) > 0 {
		g.Go(func() error { return m.store.InsertGauge(gctx, mapped.Gauge) })
	}
	if len(mapped.Sum) > 0 {
		g.Go(func() error { return m.store.InsertSum(gctx, mapped.Sum) })
	}
	if err := g.Wait(); err != nil {
		// UNAVAILABLE is retryable for OTLP exporters, and retrying is safe: a data point sent again
		// replaces the stored one instead of adding to it.
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	dataPointsStored.Add(ctx, int64(len(mapped.Gauge)), metric.WithAttributes(attribute.String("metric.type", metricTypeGauge)))
	dataPointsStored.Add(ctx, int64(len(mapped.Sum)), metric.WithAttributes(attribute.String("metric.type", metricTypeSum)))

	response := &colmetricspb.ExportMetricsServiceResponse{}
	if rejected := mapped.RejectedCount(); rejected > 0 {
		response.PartialSuccess = &colmetricspb.ExportMetricsPartialSuccess{
			RejectedDataPoints: rejected,
			ErrorMessage:       rejectionMessage(mapped.Rejected),
		}
	}
	return response, nil
}

func (m *dash0MetricsServiceServer) recordReceived(ctx context.Context, mapped MappedMetrics) {
	received := map[string]int64{
		metricTypeGauge: int64(len(mapped.Gauge)),
		metricTypeSum:   int64(len(mapped.Sum)),
	}
	for r, n := range mapped.Rejected {
		received[r.MetricType] += n
		dataPointsRejected.Add(ctx, n, metric.WithAttributes(
			attribute.String("metric.type", r.MetricType),
			attribute.String("reason", r.Reason)))
	}
	for metricType, n := range received {
		dataPointsReceived.Add(ctx, n, metric.WithAttributes(attribute.String("metric.type", metricType)))
	}
}

func rejectionMessage(rejected map[Rejection]int64) string {
	parts := make([]string, 0, len(rejected))
	for r, n := range rejected {
		parts = append(parts, fmt.Sprintf("%d %s data points: %s", n, r.MetricType, r.Reason))
	}
	slices.Sort(parts)
	return "rejected " + strings.Join(parts, ", ")
}

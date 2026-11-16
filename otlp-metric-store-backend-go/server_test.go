package main

import (
	"context"
	"errors"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeStore struct {
	mu        sync.Mutex
	series    []SeriesRow
	gauge     []DataPointRow
	sum       []DataPointRow
	seriesErr error
	gaugeErr  error
}

func (f *fakeStore) InsertSeries(_ context.Context, rows []SeriesRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.seriesErr != nil {
		return f.seriesErr
	}
	f.series = append(f.series, rows...)
	return nil
}

func (f *fakeStore) InsertGauge(_ context.Context, rows []DataPointRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.gaugeErr != nil {
		return f.gaugeErr
	}
	f.gauge = append(f.gauge, rows...)
	return nil
}

func (f *fakeStore) InsertSum(_ context.Context, rows []DataPointRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sum = append(f.sum, rows...)
	return nil
}

func (f *fakeStore) setErrors(seriesErr, gaugeErr error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seriesErr, f.gaugeErr = seriesErr, gaugeErr
}

func exportRequest(metrics ...*metricspb.Metric) *colmetricspb.ExportMetricsServiceRequest {
	return &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{
		resourceMetrics([]*commonpb.KeyValue{strAttr("service.name", "checkout")}, metrics...),
	}}
}

func TestExport_StoresSeriesAndDataPoints(t *testing.T) {
	store := &fakeStore{}
	client := newTestClient(t, store)

	out, err := client.Export(context.Background(), exportRequest(
		gaugeMetric("cpu.utilization", doublePoint(nanos(testTime), 42.5)),
		sumMetric("requests", doublePoint(nanos(testTime), 1234)),
	))
	if err != nil {
		t.Fatalf("exporting: %v", err)
	}
	if out.GetPartialSuccess() != nil {
		t.Errorf("expected no partial success, got %v", out.GetPartialSuccess())
	}
	if len(store.series) != 2 || len(store.gauge) != 1 || len(store.sum) != 1 {
		t.Fatalf("expected 2 series, 1 gauge and 1 sum row, got %d, %d and %d", len(store.series), len(store.gauge), len(store.sum))
	}
	if store.gauge[0].Value != 42.5 || store.sum[0].Value != 1234 {
		t.Errorf("unexpected values: gauge %v, sum %v", store.gauge[0].Value, store.sum[0].Value)
	}
}

func TestExport_WritesEachSeriesOncePerDay(t *testing.T) {
	store := &fakeStore{}
	client := newTestClient(t, store)
	ctx := context.Background()

	for _, at := range []time.Time{testTime, testTime.Add(time.Minute), testTime.Add(24 * time.Hour)} {
		if _, err := client.Export(ctx, exportRequest(gaugeMetric("cpu.utilization", doublePoint(nanos(at), 1)))); err != nil {
			t.Fatalf("exporting: %v", err)
		}
	}

	if len(store.gauge) != 3 {
		t.Errorf("expected 3 gauge rows, got %d", len(store.gauge))
	}
	if len(store.series) != 2 {
		t.Errorf("expected 2 series rows (one per day), got %d", len(store.series))
	}
}

func TestExport_ReportsRejectedDataPoints(t *testing.T) {
	store := &fakeStore{}
	client := newTestClient(t, store)

	noValue := doublePoint(nanos(testTime), 0)
	noValue.Value = nil
	out, err := client.Export(context.Background(), exportRequest(
		gaugeMetric("cpu.utilization", noValue, doublePoint(nanos(testTime), 1)),
		sumMetric("requests", doublePoint(0, 1)),
	))
	if err != nil {
		t.Fatalf("exporting: %v", err)
	}
	if got := out.GetPartialSuccess().GetRejectedDataPoints(); got != 2 {
		t.Errorf("expected 2 rejected data points, got %d", got)
	}
	msg := out.GetPartialSuccess().GetErrorMessage()
	if !strings.Contains(msg, rejectReasonMissingValue) || !strings.Contains(msg, rejectReasonMissingTime) {
		t.Errorf("expected both reasons in %q", msg)
	}
	if len(store.gauge) != 1 {
		t.Errorf("expected the valid data point to be stored, got %d gauge rows", len(store.gauge))
	}
}

func TestExport_StoreFailureIsRetryable(t *testing.T) {
	tests := map[string]struct {
		seriesErr error
		gaugeErr  error
		// rows stored by the failed export
		wantSeries int
	}{
		"series insert fails": {seriesErr: errors.New("connection reset"), wantSeries: 0},
		"gauge insert fails":  {gaugeErr: errors.New("connection reset"), wantSeries: 1},
	}
	for scenario, tt := range tests {
		t.Run(scenario, func(t *testing.T) {
			store := &fakeStore{seriesErr: tt.seriesErr, gaugeErr: tt.gaugeErr}
			client := newTestClient(t, store)
			ctx := context.Background()
			request := exportRequest(gaugeMetric("cpu.utilization", doublePoint(nanos(testTime), 1)))

			_, err := client.Export(ctx, request)
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("expected Unavailable, got %v", err)
			}
			if len(store.series) != tt.wantSeries || len(store.gauge) != 0 {
				t.Fatalf("expected the failed export to store %d series and no data points, got %d and %d",
					tt.wantSeries, len(store.series), len(store.gauge))
			}

			store.setErrors(nil, nil)
			if _, err := client.Export(ctx, request); err != nil {
				t.Fatalf("retrying: %v", err)
			}
			if len(store.series) != 1 || len(store.gauge) != 1 {
				t.Errorf("expected 1 series and 1 gauge row after the retry, got %d and %d", len(store.series), len(store.gauge))
			}
		})
	}
}

func TestNewServer_RejectsWindowWiderThanOneInsertAllows(t *testing.T) {
	tests := map[string]serverConfig{
		"no age":         {SeriesCacheSize: 100},
		"negative ahead": {SeriesCacheSize: 100, MaxDataPointAge: time.Hour, MaxDataPointAhead: -time.Minute},
		"99 days":        {SeriesCacheSize: 100, MaxDataPointAge: 99 * 24 * time.Hour},
	}
	for scenario, cfg := range tests {
		t.Run(scenario, func(t *testing.T) {
			if _, err := newServer(&fakeStore{}, cfg); err == nil {
				t.Errorf("expected an error for %+v", cfg)
			}
		})
	}
	if _, err := newServer(&fakeStore{}, serverConfig{SeriesCacheSize: 100, MaxDataPointAge: 98 * 24 * time.Hour}); err != nil {
		t.Errorf("expected 98 days to be accepted, got %v", err)
	}
}

func testConfig() serverConfig {
	return serverConfig{
		SeriesCacheSize:   100,
		MaxDataPointAge:   testMaxDataPointAge,
		MaxDataPointAhead: testMaxDataPointAhead,
		Now:               func() time.Time { return testNow },
	}
}

func newTestClient(t *testing.T, store MetricsStore) colmetricspb.MetricsServiceClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)

	server, err := newServer(store, testConfig())
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}
	baseServer := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(baseServer, server)
	go func() {
		if err := baseServer.Serve(lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("connecting to server: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		baseServer.Stop()
	})
	return colmetricspb.NewMetricsServiceClient(conn)
}

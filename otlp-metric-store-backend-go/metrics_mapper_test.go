package main

import (
	"fmt"
	"maps"
	"math"
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

var testTime = time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC)

// testNow is the clock tests run at: a day after testTime, so both days used in tests are accepted.
var testNow = testTime.Add(24 * time.Hour)

const (
	testMaxDataPointAge   = 7 * 24 * time.Hour
	testMaxDataPointAhead = 10 * time.Minute
)

var testWindow = TimeWindow{Oldest: testNow.Add(-testMaxDataPointAge), Newest: testNow.Add(testMaxDataPointAhead)}

func nanos(t time.Time) uint64 {
	return uint64(t.UnixNano())
}

// seriesIDOf maps a single gauge data point and returns its series ID.
func seriesIDOf(t *testing.T, resourceAttrs []*commonpb.KeyValue, pointAttrs ...*commonpb.KeyValue) SeriesID {
	t.Helper()
	mapped := MapMetrics([]*metricspb.ResourceMetrics{
		resourceMetrics(resourceAttrs, gaugeMetric("cpu.utilization", doublePoint(nanos(testTime), 1, pointAttrs...))),
	}, testWindow)
	if len(mapped.Gauge) != 1 {
		t.Fatalf("expected 1 gauge data point, got %d", len(mapped.Gauge))
	}
	return mapped.Gauge[0].SeriesID
}

func TestSeriesID_IgnoresAttributeOrder(t *testing.T) {
	a := seriesIDOf(t, []*commonpb.KeyValue{strAttr("service.name", "checkout"), strAttr("host.name", "pod-7")},
		strAttr("method", "GET"), strAttr("route", "/cart"))
	b := seriesIDOf(t, []*commonpb.KeyValue{strAttr("host.name", "pod-7"), strAttr("service.name", "checkout")},
		strAttr("route", "/cart"), strAttr("method", "GET"))
	if a != b {
		t.Errorf("expected equal series IDs, got %x and %x", a, b)
	}
}

func TestSeriesID_IgnoresNestedAttributeOrder(t *testing.T) {
	kvlist := func(kvs ...*commonpb.KeyValue) *commonpb.KeyValue {
		return &commonpb.KeyValue{Key: "labels", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{
			KvlistValue: &commonpb.KeyValueList{Values: kvs},
		}}}
	}
	a := seriesIDOf(t, nil, kvlist(strAttr("a", "1"), strAttr("b", "2")))
	b := seriesIDOf(t, nil, kvlist(strAttr("b", "2"), strAttr("a", "1")))
	if a != b {
		t.Errorf("expected equal series IDs, got %x and %x", a, b)
	}
}

func TestSeriesID_Distinguishes(t *testing.T) {
	base := seriesIDOf(t, []*commonpb.KeyValue{strAttr("service.name", "checkout")}, strAttr("port", "1"))

	tests := map[string]SeriesID{
		"value type":          seriesIDOf(t, []*commonpb.KeyValue{strAttr("service.name", "checkout")}, intAttr("port", 1)),
		"resource attribute":  seriesIDOf(t, []*commonpb.KeyValue{strAttr("service.name", "payment")}, strAttr("port", "1")),
		"attribute level":     seriesIDOf(t, []*commonpb.KeyValue{strAttr("service.name", "checkout"), strAttr("port", "1")}),
		"key/value boundary":  seriesIDOf(t, []*commonpb.KeyValue{strAttr("service.name", "checkout")}, strAttr("port1", "")),
		"extra attribute":     seriesIDOf(t, []*commonpb.KeyValue{strAttr("service.name", "checkout")}, strAttr("port", "1"), strAttr("x", "")),
		"no point attributes": seriesIDOf(t, []*commonpb.KeyValue{strAttr("service.name", "checkout")}),
	}
	for scenario, id := range tests {
		t.Run(scenario, func(t *testing.T) {
			if id == base {
				t.Errorf("expected a different series ID, got %x for both", id)
			}
		})
	}
}

func TestSeriesID_DistinguishesMetricDefinition(t *testing.T) {
	point := func() *metricspb.NumberDataPoint { return doublePoint(nanos(testTime), 1) }
	gauge := gaugeMetric("requests", point())
	sum := sumMetric("requests", point())
	described := gaugeMetric("requests", point())
	described.Description = "Requests served"

	mapped := MapMetrics([]*metricspb.ResourceMetrics{resourceMetrics(nil, gauge, sum, described)}, testWindow)
	ids := []SeriesID{mapped.Gauge[0].SeriesID, mapped.Sum[0].SeriesID, mapped.Gauge[1].SeriesID}
	if ids[0] == ids[1] || ids[0] == ids[2] || ids[1] == ids[2] {
		t.Errorf("expected distinct series IDs, got %x", ids)
	}
}

// The ID is persisted, so a change to the encoding silently splits every existing series in two.
func TestSeriesID_IsStable(t *testing.T) {
	id := seriesIDOf(t, []*commonpb.KeyValue{strAttr("service.name", "checkout")}, strAttr("method", "GET"))
	const want = "8b906a2f760d4bedecff0f1a537e47c9"
	if got := fmt.Sprintf("%x", id.BigInt()); got != want {
		t.Errorf("series ID encoding changed: want %s, got %s", want, got)
	}
}

func TestMapMetrics_OneSeriesRowPerSeriesAndDay(t *testing.T) {
	midnight := time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC)
	mapped := MapMetrics([]*metricspb.ResourceMetrics{
		resourceMetrics([]*commonpb.KeyValue{strAttr("service.name", "checkout")},
			gaugeMetric("cpu.utilization",
				doublePoint(nanos(midnight.Add(-2*time.Minute)), 1),
				doublePoint(nanos(midnight.Add(-1*time.Minute)), 2),
				doublePoint(nanos(midnight.Add(1*time.Minute)), 3),
			)),
	}, testWindow)

	if len(mapped.Gauge) != 3 {
		t.Fatalf("expected 3 gauge data points, got %d", len(mapped.Gauge))
	}
	if len(mapped.Series) != 2 {
		t.Fatalf("expected 2 series rows, got %d", len(mapped.Series))
	}
	if mapped.Series[0].SeriesID != mapped.Series[1].SeriesID {
		t.Errorf("expected both rows to describe the same series")
	}
	if want := midnight.AddDate(0, 0, -1); !mapped.Series[0].Day.Equal(want) {
		t.Errorf("expected Day=%s, got %s", want, mapped.Series[0].Day)
	}
	if !mapped.Series[1].Day.Equal(midnight) {
		t.Errorf("expected Day=%s, got %s", midnight, mapped.Series[1].Day)
	}
	s := mapped.Series[0]
	if s.ServiceName != "checkout" || s.MetricName != "cpu.utilization" || s.MetricType != metricTypeGauge {
		t.Errorf("unexpected series metadata: %+v", s)
	}
}

func TestMapMetrics_Rejects(t *testing.T) {
	noValue := doublePoint(nanos(testTime), 0)
	noValue.Value = nil
	histogram := &metricspb.Metric{Name: "latency", Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
		DataPoints: []*metricspb.HistogramDataPoint{{TimeUnixNano: nanos(testTime)}, {TimeUnixNano: nanos(testTime)}},
	}}}

	mapped := MapMetrics([]*metricspb.ResourceMetrics{resourceMetrics(nil,
		gaugeMetric("cpu.utilization", noValue, doublePoint(nanos(testTime), 1)),
		sumMetric("requests", doublePoint(0, 1)),
		histogram,
	)}, testWindow)

	want := map[Rejection]int64{
		{metricTypeGauge, rejectReasonMissingValue}:        1,
		{metricTypeSum, rejectReasonMissingTime}:           1,
		{metricTypeHistogram, rejectReasonUnsupportedType}: 2,
	}
	if !maps.Equal(mapped.Rejected, want) {
		t.Errorf("expected rejections %v, got %v", want, mapped.Rejected)
	}
	if len(mapped.Gauge) != 1 || len(mapped.Sum) != 0 || len(mapped.Series) != 1 {
		t.Errorf("expected only the valid gauge point to be mapped, got %d gauge, %d sum, %d series",
			len(mapped.Gauge), len(mapped.Sum), len(mapped.Series))
	}
}

func TestMapMetrics_RejectsTimesOutsideWindow(t *testing.T) {
	tests := map[string]struct {
		timeUnixNano uint64
		reason       string
	}{
		"oldest accepted":  {nanos(testWindow.Oldest), ""},
		"newest accepted":  {nanos(testWindow.Newest), ""},
		"too old":          {nanos(testWindow.Oldest) - 1, rejectReasonTimeTooOld},
		"in the future":    {nanos(testWindow.Newest) + 1, rejectReasonTimeInFuture},
		"beyond int64":     {math.MaxUint64, rejectReasonTimeInFuture},
		"epoch in seconds": {uint64(testTime.Unix()), rejectReasonTimeTooOld},
	}
	for scenario, tt := range tests {
		t.Run(scenario, func(t *testing.T) {
			mapped := MapMetrics([]*metricspb.ResourceMetrics{
				resourceMetrics(nil, gaugeMetric("cpu.utilization", doublePoint(tt.timeUnixNano, 1))),
			}, testWindow)

			if tt.reason == "" {
				if len(mapped.Gauge) != 1 || len(mapped.Rejected) != 0 {
					t.Errorf("expected the data point to be mapped, got %d gauge and rejections %v", len(mapped.Gauge), mapped.Rejected)
				}
				return
			}
			want := map[Rejection]int64{{metricTypeGauge, tt.reason}: 1}
			if !maps.Equal(mapped.Rejected, want) {
				t.Errorf("expected rejections %v, got %v", want, mapped.Rejected)
			}
			if len(mapped.Gauge) != 0 || len(mapped.Series) != 0 {
				t.Errorf("expected nothing to be mapped, got %d gauge and %d series", len(mapped.Gauge), len(mapped.Series))
			}
		})
	}
}

func TestMapMetrics_StoresStalenessMarkers(t *testing.T) {
	noRecordedValue := uint32(metricspb.DataPointFlags_DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK)
	withoutValue := &metricspb.NumberDataPoint{TimeUnixNano: nanos(testTime), Flags: noRecordedValue}
	withValue := doublePoint(nanos(testTime.Add(time.Minute)), 42)
	withValue.Flags = noRecordedValue

	mapped := MapMetrics([]*metricspb.ResourceMetrics{
		resourceMetrics(nil, gaugeMetric("up", withoutValue, withValue)),
	}, testWindow)

	if len(mapped.Rejected) != 0 {
		t.Errorf("expected no rejections, got %v", mapped.Rejected)
	}
	if len(mapped.Gauge) != 2 || len(mapped.Series) != 1 {
		t.Fatalf("expected 2 gauge data points of 1 series, got %d and %d", len(mapped.Gauge), len(mapped.Series))
	}
	for _, dp := range mapped.Gauge {
		if !math.IsNaN(dp.Value) || dp.Flags != noRecordedValue {
			t.Errorf("expected a NaN value with the flag kept, got value %v and flags %d", dp.Value, dp.Flags)
		}
	}
}

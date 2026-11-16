package main

import (
	"fmt"
	"math"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

const (
	metricTypeGauge                = "gauge"
	metricTypeSum                  = "sum"
	metricTypeHistogram            = "histogram"
	metricTypeExponentialHistogram = "exponential_histogram"
	metricTypeSummary              = "summary"
	metricTypeUnknown              = "unknown"
)

const (
	rejectReasonUnsupportedType = "unsupported_metric_type"
	rejectReasonMissingValue    = "missing_value"
	rejectReasonMissingTime     = "missing_time"
	rejectReasonTimeTooOld      = "time_too_old"
	rejectReasonTimeInFuture    = "time_in_future"
)

// TimeWindow is the range of data point timestamps accepted for storage. The tables are partitioned by
// day, so without it a sender with a broken clock creates a partition for every stray timestamp.
type TimeWindow struct {
	Oldest time.Time
	Newest time.Time
}

// Rejection groups rejected data points by metric type and reason.
type Rejection struct {
	MetricType string
	Reason     string
}

// MappedMetrics is an export request split into series and the data points referencing them.
type MappedMetrics struct {
	// Series holds one row per distinct (series, day) in the request.
	Series   []SeriesRow
	Gauge    []DataPointRow
	Sum      []DataPointRow
	Rejected map[Rejection]int64
}

// RejectedCount returns the total number of rejected data points.
func (m *MappedMetrics) RejectedCount() int64 {
	var n int64
	for _, c := range m.Rejected {
		n += c
	}
	return n
}

// seriesDay is a series on a given UTC day; the series table holds one row per seriesDay.
type seriesDay struct {
	ID  SeriesID
	Day int64
}

// MapMetrics converts OTLP resource metrics into series and data point rows.
// Gauge and Sum are supported; data points of other types, without a value, or with a timestamp that is
// missing or outside accepted are rejected.
func MapMetrics(resourceMetrics []*metricspb.ResourceMetrics, accepted TimeWindow) MappedMetrics {
	out := MappedMetrics{Rejected: map[Rejection]int64{}}
	seen := map[seriesDay]struct{}{}
	var enc identityEncoder

	for _, rm := range resourceMetrics {
		resource := rm.GetResource()
		enc.reset()
		enc.attributes(resource.GetAttributes())
		enc.string(rm.GetSchemaUrl())
		resourceID := enc.sum()
		svcName := serviceName(resource)
		resAttrs := kvToMap(resource.GetAttributes())

		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			enc.reset()
			enc.id(resourceID)
			enc.string(scope.GetName())
			enc.string(scope.GetVersion())
			enc.attributes(scope.GetAttributes())
			enc.uint(uint64(scope.GetDroppedAttributesCount()))
			enc.string(sm.GetSchemaUrl())
			scopeID := enc.sum()
			scopeAttrs := kvToMap(scope.GetAttributes())

			for _, metric := range sm.GetMetrics() {
				var (
					metricType  string
					dataPoints  []*metricspb.NumberDataPoint
					temporality metricspb.AggregationTemporality
					isMonotonic bool
					target      *[]DataPointRow
				)
				switch data := metric.GetData().(type) {
				case *metricspb.Metric_Gauge:
					metricType, dataPoints, target = metricTypeGauge, data.Gauge.GetDataPoints(), &out.Gauge
				case *metricspb.Metric_Sum:
					metricType, dataPoints, target = metricTypeSum, data.Sum.GetDataPoints(), &out.Sum
					temporality, isMonotonic = data.Sum.GetAggregationTemporality(), data.Sum.GetIsMonotonic()
				default:
					if unsupported, n := unsupportedMetric(metric); n > 0 {
						out.Rejected[Rejection{unsupported, rejectReasonUnsupportedType}] += n
					}
					continue
				}

				enc.reset()
				enc.id(scopeID)
				enc.string(metric.GetName())
				enc.string(metric.GetDescription())
				enc.string(metric.GetUnit())
				enc.string(metricType)
				enc.uint(uint64(temporality))
				enc.bool(isMonotonic)
				metricID := enc.sum()

				for _, dp := range dataPoints {
					if reason := rejectReason(dp, accepted); reason != "" {
						out.Rejected[Rejection{metricType, reason}]++
						continue
					}

					enc.reset()
					enc.id(metricID)
					enc.attributes(dp.GetAttributes())
					id := enc.sum()

					timeUnix := nanosToTime(dp.GetTimeUnixNano())
					day := utcDay(timeUnix)
					key := seriesDay{ID: id, Day: day.Unix()}
					if _, ok := seen[key]; !ok {
						seen[key] = struct{}{}
						out.Series = append(out.Series, SeriesRow{
							SeriesID:               id,
							Day:                    day,
							ResourceAttributes:     resAttrs,
							ResourceSchemaUrl:      rm.GetSchemaUrl(),
							ScopeName:              scope.GetName(),
							ScopeVersion:           scope.GetVersion(),
							ScopeAttributes:        scopeAttrs,
							ScopeDroppedAttrCount:  scope.GetDroppedAttributesCount(),
							ScopeSchemaUrl:         sm.GetSchemaUrl(),
							ServiceName:            svcName,
							MetricName:             metric.GetName(),
							MetricDescription:      metric.GetDescription(),
							MetricUnit:             metric.GetUnit(),
							MetricType:             metricType,
							AggregationTemporality: int32(temporality),
							IsMonotonic:            isMonotonic,
							Attributes:             kvToMap(dp.GetAttributes()),
						})
					}

					*target = append(*target, DataPointRow{
						SeriesID:      id,
						StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
						TimeUnix:      timeUnix,
						Value:         numberDataPointValue(dp),
						Flags:         dp.GetFlags(),
					})
				}
			}
		}
	}
	return out
}

// rejectReason returns why a data point can't be stored, or "" if it can.
func rejectReason(dp *metricspb.NumberDataPoint, accepted TimeWindow) string {
	// Compared as uint64 because a timestamp beyond int64 would wrap into the past as a time.Time.
	t := dp.GetTimeUnixNano()
	switch {
	// A staleness marker carries no value by design, so it is stored rather than rejected.
	case dp.GetValue() == nil && !noRecordedValue(dp):
		return rejectReasonMissingValue
	case t == 0:
		return rejectReasonMissingTime
	case t < uint64(accepted.Oldest.UnixNano()):
		return rejectReasonTimeTooOld
	case t > uint64(accepted.Newest.UnixNano()):
		return rejectReasonTimeInFuture
	}
	return ""
}

func noRecordedValue(dp *metricspb.NumberDataPoint) bool {
	return dp.GetFlags()&uint32(metricspb.DataPointFlags_DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK) != 0
}

// unsupportedMetric returns the type and number of data points of a metric that isn't stored.
func unsupportedMetric(metric *metricspb.Metric) (metricType string, dataPoints int64) {
	switch data := metric.GetData().(type) {
	case *metricspb.Metric_Histogram:
		return metricTypeHistogram, int64(len(data.Histogram.GetDataPoints()))
	case *metricspb.Metric_ExponentialHistogram:
		return metricTypeExponentialHistogram, int64(len(data.ExponentialHistogram.GetDataPoints()))
	case *metricspb.Metric_Summary:
		return metricTypeSummary, int64(len(data.Summary.GetDataPoints()))
	default:
		return metricTypeUnknown, 0
	}
}

// utcDay truncates t to midnight UTC, matching toDate() on the tables' UTC timestamps.
func utcDay(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// serviceName extracts the service.name from resource attributes, returning "" if not found.
func serviceName(resource *resourcepb.Resource) string {
	if resource == nil {
		return ""
	}
	for _, attr := range resource.GetAttributes() {
		if attr.GetKey() == "service.name" {
			return attr.GetValue().GetStringValue()
		}
	}
	return ""
}

// kvToMap converts a slice of OTLP KeyValue pairs to a Go map.
func kvToMap(attrs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		m[kv.GetKey()] = anyValueToString(kv.GetValue())
	}
	return m
}

// anyValueToString converts an OTLP AnyValue to its string representation.
func anyValueToString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return v.GetStringValue()
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", v.GetIntValue())
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", v.GetDoubleValue())
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%t", v.GetBoolValue())
	default:
		return fmt.Sprintf("%v", v)
	}
}

// nanosToTime converts a uint64 nanoseconds-since-epoch to time.Time.
func nanosToTime(nanos uint64) time.Time {
	return time.Unix(0, int64(nanos)).UTC()
}

// numberDataPointValue extracts the float64 value from a NumberDataPoint: NaN if it has no recorded value.
func numberDataPointValue(dp *metricspb.NumberDataPoint) float64 {
	if noRecordedValue(dp) {
		return math.NaN()
	}
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	default:
		return 0
	}
}

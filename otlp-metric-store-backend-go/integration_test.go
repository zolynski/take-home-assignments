//go:build integration

package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupClickHouse(t *testing.T) (*ClickHouseMetricsStore, func()) {
	t.Helper()
	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx, "clickhouse/clickhouse-server:26.2",
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithEnv(map[string]string{
			"CLICKHOUSE_USER":     "default",
			"CLICKHOUSE_PASSWORD": "test",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("9000/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("starting clickhouse container: %v", err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("getting container host: %v", err)
	}
	mappedPort, err := ctr.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("getting mapped port: %v", err)
	}

	addr := fmt.Sprintf("%s:%s", host, mappedPort.Port())
	store, err := NewClickHouseMetricsStore(ctx, addr, "default", "default", "test")
	if err != nil {
		t.Fatalf("creating clickhouse metrics store: %v", err)
	}

	cleanup := func() {
		store.Close()
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("terminating clickhouse container: %v", err)
		}
	}

	return store, cleanup
}

// gaugeInWindowSQL reads gauge values for one metric in a time window. Both tables are filtered by the
// window's days so neither is scanned beyond it; LIMIT 1 BY collapses the per-day rows of a series,
// and FINAL the data points an exporter sent more than once.
const gaugeInWindowSQL = `
SELECT s.ServiceName, s.MetricName, s.Attributes['cpu'], p.TimeUnix, p.Value
FROM otel_metrics_gauge AS p FINAL
INNER JOIN (
    SELECT SeriesId, ServiceName, MetricName, Attributes
    FROM otel_metrics_series FINAL
    WHERE Day BETWEEN toDate($1) AND toDate($2) AND MetricName = $3
    LIMIT 1 BY SeriesId
) AS s ON p.SeriesId = s.SeriesId
WHERE p.TimeUnix BETWEEN $1 AND $2
ORDER BY p.TimeUnix`

func TestClickHouseStore(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()
	ctx := context.Background()

	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	server, err := newServer(store, testConfig())
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}
	export := server.Export
	day1 := time.Date(2026, 3, 4, 10, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)

	for i, at := range []time.Time{day1, day1.Add(time.Minute), day2} {
		_, err := export(ctx, exportRequest(
			gaugeMetric("cpu.utilization", doublePoint(nanos(at), float64(i), strAttr("cpu", "0"))),
			sumMetric("requests", doublePoint(nanos(at), float64(100+i))),
		))
		if err != nil {
			t.Fatalf("exporting: %v", err)
		}
	}
	// Same series again, as a client retry after a lost response would send it.
	if _, err := export(ctx, exportRequest(gaugeMetric("cpu.utilization", doublePoint(nanos(day1), 0, strAttr("cpu", "0"))))); err != nil {
		t.Fatalf("exporting: %v", err)
	}

	t.Run("series rows are written once per series and day", func(t *testing.T) {
		var count uint64
		if err := store.conn.QueryRow(ctx, "SELECT count() FROM otel_metrics_series FINAL").Scan(&count); err != nil {
			t.Fatalf("counting series: %v", err)
		}
		if count != 4 {
			t.Errorf("expected 4 series rows (2 series x 2 days), got %d", count)
		}
	})

	t.Run("data points join their series within a time window", func(t *testing.T) {
		rows, err := store.conn.Query(ctx, gaugeInWindowSQL, day1.Add(-time.Hour), day1.Add(time.Hour), "cpu.utilization")
		if err != nil {
			t.Fatalf("querying: %v", err)
		}
		defer rows.Close()
		var values []float64
		for rows.Next() {
			var (
				svc, metric, cpu string
				at               time.Time
				value            float64
			)
			if err := rows.Scan(&svc, &metric, &cpu, &at, &value); err != nil {
				t.Fatalf("scanning: %v", err)
			}
			if svc != "checkout" || metric != "cpu.utilization" || cpu != "0" {
				t.Errorf("unexpected series metadata: %s %s cpu=%s", svc, metric, cpu)
			}
			values = append(values, value)
		}
		if fmt.Sprint(values) != "[0 1]" {
			t.Errorf("expected values [0 1] from day 1 only, with the retried data point stored once, got %v", values)
		}
	})

	t.Run("time-bounded queries use partition and primary key pruning", func(t *testing.T) {
		// ClickHouse refuses to run these queries if they would read every partition or ignore the primary key.
		forceIndexes := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"force_index_by_date": 1,
		}))
		rows, err := store.conn.Query(forceIndexes, gaugeInWindowSQL, day1.Add(-time.Hour), day1.Add(time.Hour), "cpu.utilization")
		if err != nil {
			t.Fatalf("join query does not prune by date: %v", err)
		}
		rows.Close()

		forcePrimaryKey := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"force_index_by_date": 1,
			"force_primary_key":   1,
		}))
		for _, table := range []string{gaugeTable, sumTable} {
			var count uint64
			err := store.conn.QueryRow(forcePrimaryKey, "SELECT count() FROM "+table+" WHERE TimeUnix BETWEEN $1 AND $2",
				day1.Add(-time.Hour), day1.Add(time.Hour)).Scan(&count)
			if err != nil {
				t.Fatalf("time-only query on %s does not use the primary key: %v", table, err)
			}
		}
	})
}

func TestClickHouseStore_CombinesConcurrentExports(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()
	ctx := context.Background()

	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}
	// Without merges, every insert stays visible as one part.
	if err := store.conn.Exec(ctx, "SYSTEM STOP MERGES"); err != nil {
		t.Fatalf("stopping merges: %v", err)
	}
	batching, err := NewBatchingMetricsStore(store, BatchConfig{MaxRows: 100_000, MaxDelay: time.Second})
	if err != nil {
		t.Fatalf("creating batching store: %v", err)
	}
	defer batching.Close()
	server, err := newServer(batching, testConfig())
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}

	const exports = 50
	errs := make([]error, exports)
	var wg sync.WaitGroup
	for i := range exports {
		wg.Go(func() {
			_, errs[i] = server.Export(ctx, exportRequest(
				gaugeMetric("cpu.utilization", doublePoint(nanos(testTime.Add(time.Duration(i)*time.Second)), float64(i))),
			))
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("export %d: %v", i, err)
		}
	}

	var rows, parts uint64
	if err := store.conn.QueryRow(ctx, "SELECT count() FROM otel_metrics_gauge").Scan(&rows); err != nil {
		t.Fatalf("counting data points: %v", err)
	}
	if rows != exports {
		t.Errorf("expected %d data points, got %d", exports, rows)
	}
	err = store.conn.QueryRow(ctx,
		"SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = $1 AND active", gaugeTable).Scan(&parts)
	if err != nil {
		t.Fatalf("counting parts: %v", err)
	}
	// One part per export is what batching exists to avoid; a few are allowed in case scheduling splits the batch.
	if parts > 5 {
		t.Errorf("expected the %d exports to share a few inserts, got %d parts", exports, parts)
	}
}

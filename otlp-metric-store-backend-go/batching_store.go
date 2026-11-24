package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// insertTimeout bounds one insert, which serves many exports and so can't run on any one export's context.
const insertTimeout = 30 * time.Second

var errStoreClosed = errors.New("metrics store is closed")

// BatchConfig controls how the rows of concurrent exports are combined into inserts.
type BatchConfig struct {
	// MaxRows sends a table's insert once this many rows are waiting.
	MaxRows int
	// MaxDelay sends it after this long even if fewer rows are waiting.
	MaxDelay time.Duration
}

// BatchingMetricsStore combines the rows of concurrent exports into few large inserts, because ClickHouse
// creates a part per insert and falls behind merging them when small ones arrive quickly.
// An insert returns once the batch holding its rows is stored, so an export is still acknowledged only
// after its data is.
type BatchingMetricsStore struct {
	series    *batcher[SeriesRow]
	gauge     *batcher[DataPointRow]
	sum       *batcher[DataPointRow]
	closeOnce sync.Once
}

// NewBatchingMetricsStore starts batching inserts into store. Close it before closing store.
func NewBatchingMetricsStore(store MetricsStore, cfg BatchConfig) (*BatchingMetricsStore, error) {
	if cfg.MaxRows <= 0 || cfg.MaxDelay <= 0 {
		return nil, fmt.Errorf("insertBatchMaxRows and insertBatchMaxDelay must be positive")
	}
	return &BatchingMetricsStore{
		series: newBatcher(seriesTable, cfg, store.InsertSeries),
		gauge:  newBatcher(gaugeTable, cfg, store.InsertGauge),
		sum:    newBatcher(sumTable, cfg, store.InsertSum),
	}, nil
}

func (s *BatchingMetricsStore) InsertSeries(ctx context.Context, rows []SeriesRow) error {
	return s.series.add(ctx, rows)
}

func (s *BatchingMetricsStore) InsertGauge(ctx context.Context, rows []DataPointRow) error {
	return s.gauge.add(ctx, rows)
}

func (s *BatchingMetricsStore) InsertSum(ctx context.Context, rows []DataPointRow) error {
	return s.sum.add(ctx, rows)
}

// Close inserts the rows still waiting and stops batching. The wrapped store stays open.
func (s *BatchingMetricsStore) Close() {
	s.closeOnce.Do(func() {
		// Closing wakes each run loop and every add still waiting to hand over rows.
		close(s.series.closed)
		close(s.gauge.closed)
		close(s.sum.closed)
		// Each run closes stopped once its last batch is inserted.
		<-s.series.stopped
		<-s.gauge.stopped
		<-s.sum.stopped
	})
}

type batchItem[T any] struct {
	rows []T
	done chan<- error
}

// batcher runs one insert at a time for its table. While that insert is in flight it takes no rows,
// which holds exports back instead of buffering without limit.
type batcher[T any] struct {
	table   string
	cfg     BatchConfig
	insert  func(context.Context, []T) error
	items   chan batchItem[T]
	closed  chan struct{}
	stopped chan struct{}
}

func newBatcher[T any](table string, cfg BatchConfig, insert func(context.Context, []T) error) *batcher[T] {
	b := &batcher[T]{
		table:   table,
		cfg:     cfg,
		insert:  insert,
		items:   make(chan batchItem[T]),
		closed:  make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go b.run()
	return b
}

// add queues rows and waits for the insert holding them to finish.
func (b *batcher[T]) add(ctx context.Context, rows []T) error {
	if len(rows) == 0 {
		return nil
	}
	// Buffered, so flush can deliver the result even if this export has stopped waiting.
	done := make(chan error, 1)
	// Blocks while the batcher is inserting the previous batch: exports wait instead of piling up rows.
	select {
	case b.items <- batchItem[T]{rows: rows, done: done}:
	case <-ctx.Done():
		return ctx.Err()
	case <-b.closed:
		return errStoreClosed
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// The rows are inserted regardless; a retried export replaces them.
		return ctx.Err()
	}
}

func (b *batcher[T]) run() {
	defer close(b.stopped)
	// One batch at a time: collect, insert, repeat. Rows collected before close are still inserted.
	for {
		rows, waiters, closed := b.collect()
		if len(waiters) > 0 {
			b.flush(rows, waiters)
		}
		if closed {
			return
		}
	}
}

// collect waits for the first rows, then gathers more until the batch is full or MaxDelay has passed.
func (b *batcher[T]) collect() (rows []T, waiters []chan<- error, closed bool) {
	// Nil until the first rows arrive, and a nil channel never fires: an idle batcher has no timer.
	var deadline <-chan time.Time
	for len(rows) < b.cfg.MaxRows {
		select {
		case item := <-b.items:
			// The delay counts from the first rows, so none wait longer than MaxDelay for others.
			if deadline == nil {
				deadline = time.After(b.cfg.MaxDelay)
			}
			rows = append(rows, item.rows...)
			waiters = append(waiters, item.done)
		case <-deadline:
			return rows, waiters, false
		case <-b.closed:
			return rows, waiters, true
		}
	}
	return rows, waiters, false
}

func (b *batcher[T]) flush(rows []T, waiters []chan<- error) {
	ctx, cancel := context.WithTimeout(context.Background(), insertTimeout)
	defer cancel()

	start := time.Now()
	err := b.insert(ctx, rows)
	tableAttr := metric.WithAttributes(attribute.String("table", b.table))
	clickhouseInsertDuration.Record(ctx, time.Since(start).Seconds(), tableAttr)
	clickhouseInsertRows.Record(ctx, int64(len(rows)), tableAttr)
	if err != nil {
		clickhouseInsertErrors.Add(ctx, 1, tableAttr)
		slog.ErrorContext(ctx, "ClickHouse insert failed", slog.String("table", b.table),
			slog.Int("rows", len(rows)), slog.Int("exports", len(waiters)), slog.Any("error", err))
		err = fmt.Errorf("inserting into %s: %w", b.table, err)
	}
	// Every export in the batch gets the same outcome; done's buffer means this never blocks.
	for _, done := range waiters {
		done <- err
	}
}

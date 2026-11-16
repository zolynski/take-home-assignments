package main

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

// recordingStore records the size of every insert it receives.
type recordingStore struct {
	mu      sync.Mutex
	inserts []int
	err     error
}

func (r *recordingStore) record(rows int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inserts = append(r.inserts, rows)
	return r.err
}

func (r *recordingStore) recorded() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.inserts)
}

func (r *recordingStore) InsertSeries(_ context.Context, rows []SeriesRow) error {
	return r.record(len(rows))
}

func (r *recordingStore) InsertGauge(_ context.Context, rows []DataPointRow) error {
	return r.record(len(rows))
}

func (r *recordingStore) InsertSum(_ context.Context, rows []DataPointRow) error {
	return r.record(len(rows))
}

func newTestBatchingStore(t *testing.T, store MetricsStore, cfg BatchConfig) *BatchingMetricsStore {
	t.Helper()
	batching, err := NewBatchingMetricsStore(store, cfg)
	if err != nil {
		t.Fatalf("creating batching store: %v", err)
	}
	t.Cleanup(batching.Close)
	return batching
}

// insertConcurrently calls InsertGauge from n goroutines with rowsEach rows and returns their errors.
func insertConcurrently(batching *BatchingMetricsStore, n, rowsEach int) []error {
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			errs[i] = batching.InsertGauge(context.Background(), make([]DataPointRow, rowsEach))
		})
	}
	wg.Wait()
	return errs
}

func TestBatchingStore_CombinesConcurrentInserts(t *testing.T) {
	store := &recordingStore{}
	// The delay is never reached: the batch is sent because the 20 callers fill it.
	batching := newTestBatchingStore(t, store, BatchConfig{MaxRows: 100, MaxDelay: time.Hour})

	for i, err := range insertConcurrently(batching, 20, 5) {
		if err != nil {
			t.Errorf("insert %d: %v", i, err)
		}
	}
	if got := store.recorded(); !slices.Equal(got, []int{100}) {
		t.Errorf("expected one insert of 100 rows, got %v", got)
	}
}

func TestBatchingStore_SendsPartialBatchAfterDelay(t *testing.T) {
	store := &recordingStore{}
	batching := newTestBatchingStore(t, store, BatchConfig{MaxRows: 100, MaxDelay: 10 * time.Millisecond})

	if err := batching.InsertSum(context.Background(), make([]DataPointRow, 3)); err != nil {
		t.Fatalf("inserting: %v", err)
	}
	if got := store.recorded(); !slices.Equal(got, []int{3}) {
		t.Errorf("expected one insert of 3 rows, got %v", got)
	}
}

func TestBatchingStore_FailureReachesEveryCaller(t *testing.T) {
	store := &recordingStore{err: errors.New("connection reset")}
	batching := newTestBatchingStore(t, store, BatchConfig{MaxRows: 100, MaxDelay: time.Hour})

	for i, err := range insertConcurrently(batching, 20, 5) {
		if !errors.Is(err, store.err) {
			t.Errorf("insert %d: expected the store's error, got %v", i, err)
		}
	}
}

func TestBatchingStore_CloseInsertsWaitingRows(t *testing.T) {
	store := &recordingStore{}
	batching := newTestBatchingStore(t, store, BatchConfig{MaxRows: 100, MaxDelay: time.Hour})

	// The export gives up before its batch is sent, as a client with a short deadline would.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := batching.InsertGauge(ctx, make([]DataPointRow, 3)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the caller's deadline to end the wait, got %v", err)
	}

	batching.Close()
	if got := store.recorded(); !slices.Equal(got, []int{3}) {
		t.Errorf("expected the waiting rows to be inserted on close, got %v", got)
	}
	if err := batching.InsertGauge(context.Background(), make([]DataPointRow, 1)); !errors.Is(err, errStoreClosed) {
		t.Errorf("expected errStoreClosed after close, got %v", err)
	}
}

func TestNewBatchingMetricsStore_RejectsInvalidConfig(t *testing.T) {
	valid := BatchConfig{MaxRows: 1, MaxDelay: time.Millisecond}
	tests := map[string]func(*BatchConfig){
		"no rows":  func(c *BatchConfig) { c.MaxRows = 0 },
		"no delay": func(c *BatchConfig) { c.MaxDelay = 0 },
	}
	for scenario, breakIt := range tests {
		t.Run(scenario, func(t *testing.T) {
			cfg := valid
			breakIt(&cfg)
			if _, err := NewBatchingMetricsStore(&recordingStore{}, cfg); err == nil {
				t.Errorf("expected an error for %+v", cfg)
			}
		})
	}
}

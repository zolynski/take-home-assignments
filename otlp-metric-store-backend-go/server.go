package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var (
	listenAddr            = flag.String("listenAddr", "localhost:4317", "The listen address")
	maxReceiveMessageSize = flag.Int("maxReceiveMessageSize", 16777216, "The max message size in bytes the server can receive")
	clickhouseAddr        = flag.String("clickhouseAddr", "localhost:9000", "The ClickHouse native protocol address")
	clickhouseDatabase    = flag.String("clickhouseDatabase", "default", "The ClickHouse database")
	clickhouseUser        = flag.String("clickhouseUser", "default", "The ClickHouse user; the password is read from CLICKHOUSE_PASSWORD")
	seriesCacheSize       = flag.Int("seriesCacheSize", 1_000_000, "The number of (series, day) pairs remembered as already written")
	maxDataPointAge       = flag.Duration("maxDataPointAge", 7*24*time.Hour, "Data points older than this are rejected")
	maxDataPointAhead     = flag.Duration("maxDataPointAhead", 10*time.Minute, "Data points further in the future than this are rejected")
	insertBatchMaxRows    = flag.Int("insertBatchMaxRows", 100_000, "Rows of concurrent exports combined into one insert before it is sent")
	insertBatchMaxDelay   = flag.Duration("insertBatchMaxDelay", 200*time.Millisecond, "The longest an insert waits for more rows")
)

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	flag.Parse()
	slog.SetDefault(logger)
	logger.Info("Starting application")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Set up OpenTelemetry.
	otelShutdown, err := setupOTelSDK(ctx)
	if err != nil {
		return
	}

	// Handle shutdown properly so nothing leaks.
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	slog.Debug("Connecting to ClickHouse", slog.String("clickhouseAddr", *clickhouseAddr))
	store, err := NewClickHouseMetricsStore(ctx, *clickhouseAddr, *clickhouseDatabase, *clickhouseUser, os.Getenv("CLICKHOUSE_PASSWORD"))
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, store.Close())
	}()
	if err = store.CreateTables(ctx); err != nil {
		return err
	}

	batchingStore, err := NewBatchingMetricsStore(store, BatchConfig{MaxRows: *insertBatchMaxRows, MaxDelay: *insertBatchMaxDelay})
	if err != nil {
		return err
	}
	// Deferred after the ClickHouse store's Close, so the last batches are inserted before the connections go.
	defer batchingStore.Close()

	server, err := newServer(batchingStore, serverConfig{
		SeriesCacheSize:   *seriesCacheSize,
		MaxDataPointAge:   *maxDataPointAge,
		MaxDataPointAhead: *maxDataPointAhead,
	})
	if err != nil {
		return err
	}

	slog.Debug("Starting listener", slog.String("listenAddr", *listenAddr))
	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(*maxReceiveMessageSize),
		grpc.Creds(insecure.NewCredentials()),
	)
	colmetricspb.RegisterMetricsServiceServer(grpcServer, server)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	// Let in-flight exports finish their inserts before the store is closed.
	go func() {
		<-ctx.Done()
		slog.Info("Shutting down gRPC server")
		// Report NOT_SERVING first, so load balancers stop sending exports while the in-flight ones finish.
		healthServer.Shutdown()
		grpcServer.GracefulStop()
	}()

	slog.Debug("Starting gRPC server")

	return grpcServer.Serve(listener)
}

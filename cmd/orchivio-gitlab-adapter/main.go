// Command orchivio-gitlab-adapter serves the plugin.v1 CodeHostAdapter for
// one GitLab instance (Self-Managed or GitLab.com).
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc"

	"github.com/orchivio/adapter-gitlab/internal/adapter"
)

const shutdownTimeout = 10 * time.Second

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Error("gitlab adapter failed", "err", err)
		os.Exit(1)
	}
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func run() error {
	interval, err := time.ParseDuration(env("GITLAB_POLL_INTERVAL", "30s"))
	if err != nil {
		return err
	}
	srv, err := adapter.New(adapter.Config{URL: os.Getenv("GITLAB_URL"), PollInterval: interval})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	lis, err := net.Listen("tcp", env("ORCHIVIO_GRPC_ADDR", "127.0.0.1:7401"))
	if err != nil {
		return err
	}
	// ponytail: insecure gRPC, bind localhost; mTLS before remote workers.
	g := grpc.NewServer()
	pb.RegisterCodeHostAdapterServer(g, srv)
	errs := make(chan error, 1)
	go func() { errs <- g.Serve(lis) }()
	slog.Info("gitlab adapter started", "grpc", lis.Addr().String())

	select {
	case err = <-errs:
	case <-ctx.Done():
	}
	srv.Close()
	done := make(chan struct{})
	go func() { g.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(shutdownTimeout):
		g.Stop()
	}
	return err
}

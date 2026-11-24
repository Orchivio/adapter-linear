// Command orchivio-linear-adapter serves the TrackerAdapter gRPC API for Linear.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pluginv1 "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"github.com/orchivio/orchestrator/contract/secretref"
	"google.golang.org/grpc"

	"github.com/orchivio/adapter-linear/internal/linear"
	"github.com/orchivio/adapter-linear/internal/server"
)

const shutdownTimeout = 10 * time.Second

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Error("adapter failed", "err", err)
		os.Exit(1)
	}
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// newOAuth configures the installation's Linear OAuth app; workspaces connect through it.
func newOAuth(resolver secretref.Env) (*linear.OAuth, error) {
	id := os.Getenv("LINEAR_CLIENT_ID")
	if id == "" {
		return nil, errors.New("LINEAR_CLIENT_ID is required")
	}
	secret, err := resolver.Resolve(os.Getenv("LINEAR_CLIENT_SECRET_REF"))
	if err != nil {
		return nil, err
	}
	return &linear.OAuth{
		APIURL:       env("LINEAR_API_URL", linear.DefaultAPIURL),
		OAuthURL:     env("LINEAR_OAUTH_URL", linear.DefaultOAuthURL),
		ClientID:     id,
		ClientSecret: secret,
		Now:          time.Now,
	}, nil
}

func run() error {
	resolver := secretref.Env{Lookup: os.LookupEnv}
	oauth, err := newOAuth(resolver)
	if err != nil {
		return err
	}
	webhookSecret := ""
	if ref := os.Getenv("LINEAR_WEBHOOK_SECRET_REF"); ref != "" {
		if webhookSecret, err = resolver.Resolve(ref); err != nil {
			return err
		}
	}
	interval, err := time.ParseDuration(env("LINEAR_POLL_INTERVAL", "15s"))
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	hub := server.NewHub()
	lis, err := net.Listen("tcp", env("ORCHIVIO_GRPC_ADDR", "127.0.0.1:7101"))
	if err != nil {
		return err
	}
	// ponytail: insecure gRPC, bind localhost; mTLS before remote workers.
	grpcServer := grpc.NewServer()
	adapter := server.New(oauth, hub, interval, webhookSecret != "")
	pluginv1.RegisterTrackerAdapterServer(grpcServer, adapter)
	httpServer := &http.Server{Addr: env("ORCHIVIO_WEBHOOK_ADDR", ":7102"), Handler: server.WebhookHandler(webhookSecret, hub), ReadHeaderTimeout: 10 * time.Second}

	errs := make(chan error, 2)
	go func() { errs <- grpcServer.Serve(lis) }()
	go func() { errs <- httpServer.ListenAndServe() }()
	slog.Info("linear adapter started", "webhooks", webhookSecret != "")

	select {
	case err = <-errs:
	case <-ctx.Done():
	}
	adapter.Close()
	shutdown(grpcServer, httpServer)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func shutdown(g *grpc.Server, h *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	done := make(chan struct{})
	go func() { g.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		g.Stop()
	}
	_ = h.Shutdown(ctx)
}

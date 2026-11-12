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
	"strings"
	"syscall"
	"time"

	pluginv1 "github.com/soemii/orchivio-server/contract/gen/orchivio/plugin/v1"
	"github.com/soemii/orchivio-server/contract/secretref"
	"google.golang.org/grpc"

	"github.com/soemii/orchivio-adapter-linear/internal/linear"
	"github.com/soemii/orchivio-adapter-linear/internal/server"
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

// newClient acts as the OAuth app user when LINEAR_CLIENT_ID is set,
// otherwise as the owner of a personal API key.
func newClient(resolver secretref.Env) (*linear.Client, error) {
	if id := os.Getenv("LINEAR_CLIENT_ID"); id != "" {
		secret, err := resolver.Resolve(os.Getenv("LINEAR_CLIENT_SECRET_REF"))
		if err != nil {
			return nil, err
		}
		return linear.NewApp(linear.DefaultEndpoint, env("LINEAR_TOKEN_URL", linear.DefaultTokenURL), id, secret), nil
	}
	apiKey, err := resolver.Resolve(os.Getenv("LINEAR_API_KEY_REF"))
	if err != nil {
		return nil, err
	}
	return linear.New(linear.DefaultEndpoint, apiKey), nil
}

func run() error {
	resolver := secretref.Env{Lookup: os.LookupEnv}
	client, err := newClient(resolver)
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
	var teams []string
	for _, t := range strings.Split(os.Getenv("LINEAR_TEAM_KEYS"), ",") {
		if t = strings.TrimSpace(t); t != "" {
			teams = append(teams, t)
		}
	}

	if len(teams) == 0 {
		return errors.New("LINEAR_TEAM_KEYS must list at least one team key")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if _, err := client.Viewer(ctx); err != nil { // fail fast on a bad key
		return err
	}

	hub := server.NewHub()
	lis, err := net.Listen("tcp", env("ORCHIVIO_GRPC_ADDR", "127.0.0.1:7101"))
	if err != nil {
		return err
	}
	// ponytail: insecure gRPC, bind localhost; mTLS before remote workers.
	grpcServer := grpc.NewServer()
	adapter := server.New(client, hub, teams, interval, webhookSecret != "", time.Now)
	pluginv1.RegisterTrackerAdapterServer(grpcServer, adapter)
	httpServer := &http.Server{Addr: env("ORCHIVIO_WEBHOOK_ADDR", ":7102"), Handler: server.WebhookHandler(webhookSecret, hub), ReadHeaderTimeout: 10 * time.Second}

	errs := make(chan error, 2)
	go func() { errs <- grpcServer.Serve(lis) }()
	go func() { errs <- httpServer.ListenAndServe() }()
	slog.Info("linear adapter started", "teams", teams, "webhooks", webhookSecret != "")

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

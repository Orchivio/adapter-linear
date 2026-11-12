package server

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pluginv1 "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/orchivio/adapter-linear/internal/linear"
)

func TestLateStreamSeesEventSinceProcessStart(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	lin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query     string
			Variables struct{ Since string }
		}
		json.NewDecoder(r.Body).Decode(&req)
		switch {
		case strings.Contains(req.Query, "viewer"):
			w.Write([]byte(`{"data":{"viewer":{"id":"bot","displayName":"orchivio"}}}`))
		case strings.Contains(req.Query, "issues("):
			w.Write([]byte(`{"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":false}}}}`))
		default:
			nodes := `[]`
			if since, _ := time.Parse(time.RFC3339Nano, req.Variables.Since); since.Before(time.Date(2026, 1, 1, 12, 1, 0, 0, time.UTC)) {
				nodes = `[{"id":"c1","body":"hi","createdAt":"2026-01-01T12:01:00Z","user":{"id":"u1"},"issue":{"identifier":"ORC-1","team":{"key":"ORC"}}}]`
			}
			w.Write([]byte(`{"data":{"comments":{"nodes":` + nodes + `,"pageInfo":{"hasNextPage":false}}}}`))
		}
	}))
	defer lin.Close()
	late := false
	now := func() time.Time {
		if late {
			return t0.Add(time.Hour)
		}
		return t0
	}
	srv := New(linear.New(lin.URL, "k"), NewHub(), []string{"ORC"}, time.Hour, false, now)
	late = true // any now() call after construction is much later
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream := &recvStream{ctx: ctx, got: make(chan *pluginv1.TrackerEvent, 1)}
	go srv.StreamEvents(&pluginv1.StreamEventsRequest{}, stream)
	select {
	case ev := <-stream.got:
		if ev.ExternalId != "comment:c1" {
			t.Fatalf("ev=%v", ev)
		}
	case <-ctx.Done():
		t.Fatal("late stream missed event created after process start")
	}
}

func TestCloseEndsOpenStreams(t *testing.T) {
	lin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Query string }
		json.NewDecoder(r.Body).Decode(&req)
		if strings.Contains(req.Query, "viewer") {
			w.Write([]byte(`{"data":{"viewer":{"id":"bot"}}}`))
			return
		}
		w.Write([]byte(`{"data":{"issues":{"nodes":[],"pageInfo":{}},"comments":{"nodes":[],"pageInfo":{}}}}`))
	}))
	defer lin.Close()
	srv := New(linear.New(lin.URL, "k"), NewHub(), []string{"ORC"}, time.Hour, false, time.Now)
	done := make(chan error, 1)
	go func() {
		done <- srv.StreamEvents(&pluginv1.StreamEventsRequest{}, &recvStream{ctx: context.Background(), got: make(chan *pluginv1.TrackerEvent, 1)})
	}()
	srv.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stream did not end after Close")
	}
}

type recvStream struct {
	grpc.ServerStream
	ctx context.Context
	got chan *pluginv1.TrackerEvent
}

func (s *recvStream) Context() context.Context { return s.ctx }
func (s *recvStream) Send(ev *pluginv1.TrackerEvent) error {
	select {
	case s.got <- ev:
	default:
	}
	return nil
}

func TestStreamEventsPollsThenNudge(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	polls := 0
	lin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Query string }
		json.NewDecoder(r.Body).Decode(&req)
		switch {
		case strings.Contains(req.Query, "viewer"):
			w.Write([]byte(`{"data":{"viewer":{"id":"bot","displayName":"orchivio"}}}`))
		case strings.Contains(req.Query, "issues("):
			w.Write([]byte(`{"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":false}}}}`))
		default: // comments: one distinct comment per poll
			polls++
			w.Write([]byte(`{"data":{"comments":{"nodes":[{"id":"c` + string(rune('0'+polls)) + `","body":"hi","createdAt":"2026-01-01T12:01:00Z","user":{"id":"u1","email":"u@x","name":"U"},"issue":{"identifier":"ORC-1","team":{"key":"ORC"}}}],"pageInfo":{"hasNextPage":false}}}}`))
		}
	}))
	defer lin.Close()

	hub := NewHub()
	// interval of one hour: only the webhook nudge can trigger the second poll
	srv := New(linear.New(lin.URL, "k"), hub, []string{"ORC"}, time.Hour, true, func() time.Time { return t0 })
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer()
	pluginv1.RegisterTrackerAdapterServer(g, srv)
	go g.Serve(lis)
	defer g.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pluginv1.NewTrackerAdapterClient(conn)

	caps, err := client.GetCapabilities(context.Background(), &pluginv1.GetCapabilitiesRequest{})
	if err != nil || caps.ContractVersion != "plugin.v1" || caps.AdapterName != "linear" || !caps.SupportsWebhooks || caps.SupportsThreads {
		t.Fatalf("caps=%v err=%v", caps, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := client.StreamEvents(ctx, &pluginv1.StreamEventsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil || first.ExternalId != "comment:c1" {
		t.Fatalf("first=%v err=%v", first, err)
	}
	// stream is now waiting on the hour-long interval; a webhook nudge wakes it
	body := "{}"
	if code := post(WebhookHandler("s", hub), sign("s", body), body); code != http.StatusOK {
		t.Fatalf("webhook code %d", code)
	}
	second, err := stream.Recv()
	if err != nil || second.ExternalId != "comment:c2" {
		t.Fatalf("second=%v err=%v", second, err)
	}
}

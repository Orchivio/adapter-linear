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

	pluginv1 "github.com/soemii/orchivio-server/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/soemii/orchivio-adapter-linear/internal/linear"
)

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

package server

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	pluginv1 "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/orchivio/adapter-linear/internal/linear"
)

func TestStreamWithoutKeysIsInvalidArgument(t *testing.T) {
	srv := newServer("http://127.0.0.1:0", NewHub(), false, time.Now)
	defer srv.Close()
	err := srv.StreamEvents(&pluginv1.StreamEventsRequest{}, &recvStream{ctx: context.Background(), got: make(chan *pluginv1.TrackerEvent, 1)})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
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
	srv := newServer(lin.URL, NewHub(), false, time.Now)
	done := make(chan error, 1)
	go func() {
		done <- srv.StreamEvents(&pluginv1.StreamEventsRequest{ProjectKeys: []string{"ORC"}, Credential: cred("at-1", "org-1")}, &recvStream{ctx: context.Background(), got: make(chan *pluginv1.TrackerEvent, 1)})
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
	srv := newServer(lin.URL, hub, true, func() time.Time { return t0 })
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
	stream, err := client.StreamEvents(ctx, &pluginv1.StreamEventsRequest{ProjectKeys: []string{"ORC"}, Credential: cred("at-1", "org-1")})
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

// newServer points the adapter at a fake Linear serving OAuth and GraphQL.
func newServer(apiURL string, hub *Hub, webhooks bool, now func() time.Time) *Server {
	oauth := &linear.OAuth{APIURL: apiURL, OAuthURL: "https://linear.example", ClientID: "cid", ClientSecret: "csecret", Now: now}
	return New(oauth, hub, time.Hour, webhooks)
}

func cred(token, org string) *pluginv1.Credential {
	data, _ := json.Marshal(linear.Credential{AccessToken: token, RefreshToken: "rt-1", OrganizationID: org})
	return &pluginv1.Credential{Data: data}
}

var issued = time.Date(2026, 11, 24, 9, 0, 0, 0, time.UTC)

// fakeLinear answers the token endpoint with tokenStatus/tokenBody and GraphQL
// from graphql (by query content), recording Authorization headers.
type fakeLinear struct {
	tokenStatus int
	tokenBody   string
	teams       string
	auths       []string
}

func (f *fakeLinear) server(t *testing.T) *Server {
	lin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/revoke" {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		if r.URL.Path == "/oauth/token" {
			w.WriteHeader(f.tokenStatus)
			w.Write([]byte(f.tokenBody))
			return
		}
		f.auths = append(f.auths, r.Header.Get("Authorization"))
		var req struct{ Query string }
		json.NewDecoder(r.Body).Decode(&req)
		switch {
		case strings.Contains(req.Query, "organization"):
			w.Write([]byte(`{"data":{"organization":{"id":"org-1","name":"Cubie Studio"}}}`))
		case strings.Contains(req.Query, "teams"):
			w.Write([]byte(`{"data":{"teams":{"nodes":` + f.teams + `,"pageInfo":{"hasNextPage":false}}}}`))
		default:
			w.Write([]byte(`{"data":{"issue":{"id":"u","identifier":"ORC-1","title":"T","labels":{"nodes":[]}}}}`))
		}
	}))
	t.Cleanup(lin.Close)
	srv := newServer(lin.URL, NewHub(), false, func() time.Time { return issued })
	t.Cleanup(srv.Close)
	return srv
}

func TestBeginConnectURL(t *testing.T) {
	srv := (&fakeLinear{}).server(t)
	resp, err := srv.BeginConnect(context.Background(), &pluginv1.BeginConnectRequest{Tenant: "acme", RedirectUri: "https://o/oauth/callback/linear", State: "st-1"})
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(resp.GetUrl())
	q := u.Query()
	if q.Get("actor") != "app" || q.Get("scope") != "read,write,app:assignable,app:mentionable" || q.Get("state") != "st-1" ||
		q.Get("redirect_uri") != "https://o/oauth/callback/linear" || !strings.HasPrefix(resp.GetUrl(), "https://linear.example/oauth/authorize?") {
		t.Fatalf("url = %s", resp.GetUrl())
	}
}

func TestCompleteConnectReturnsWorkspaceCredential(t *testing.T) {
	srv := (&fakeLinear{tokenStatus: http.StatusOK, tokenBody: `{"access_token":"at-1","refresh_token":"rt-1","expires_in":86399}`}).server(t)
	resp, err := srv.CompleteConnect(context.Background(), &pluginv1.CompleteConnectRequest{RedirectUri: "https://o/cb", Query: map[string]string{"code": "c", "state": "s"}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetExternalId() != "org-1" || resp.GetLabel() != "Cubie Studio" {
		t.Fatalf("resp = %v", resp)
	}
	if got := resp.GetCredential().GetRefreshAfter().AsTime(); !got.Equal(issued.Add(12 * time.Hour)) {
		t.Fatalf("refresh_after = %v", got)
	}
	var data linear.Credential
	if err := json.Unmarshal(resp.GetCredential().GetData(), &data); err != nil || data.AccessToken != "at-1" || data.OrganizationID != "org-1" {
		t.Fatalf("data = %+v err=%v", data, err)
	}
}

func TestCompleteConnectWithoutCodeIsInvalidArgument(t *testing.T) {
	srv := (&fakeLinear{}).server(t)
	_, err := srv.CompleteConnect(context.Background(), &pluginv1.CompleteConnectRequest{Query: map[string]string{"error": "access_denied"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v", err)
	}
}

func TestRefreshInvalidGrantIsUnauthenticated(t *testing.T) {
	srv := (&fakeLinear{tokenStatus: http.StatusBadRequest, tokenBody: `{"error":"invalid_grant"}`}).server(t)
	_, err := srv.RefreshCredential(context.Background(), &pluginv1.RefreshCredentialRequest{Credential: cred("at-1", "org-1")})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("err = %v", err)
	}
}

func TestRefreshServerErrorIsUnavailable(t *testing.T) {
	srv := (&fakeLinear{tokenStatus: http.StatusServiceUnavailable}).server(t)
	_, err := srv.RefreshCredential(context.Background(), &pluginv1.RefreshCredentialRequest{Credential: cred("at-1", "org-1")})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v", err)
	}
}

func TestRefreshReturnsNewCredential(t *testing.T) {
	srv := (&fakeLinear{tokenStatus: http.StatusOK, tokenBody: `{"access_token":"at-2","refresh_token":"rt-2","expires_in":86399}`}).server(t)
	got, err := srv.RefreshCredential(context.Background(), &pluginv1.RefreshCredentialRequest{Credential: cred("at-1", "org-1")})
	if err != nil {
		t.Fatal(err)
	}
	var data linear.Credential
	json.Unmarshal(got.GetData(), &data)
	if data.AccessToken != "at-2" || data.OrganizationID != "org-1" || !got.GetRefreshAfter().AsTime().Equal(issued.Add(12*time.Hour)) {
		t.Fatalf("data=%+v refresh_after=%v", data, got.GetRefreshAfter().AsTime())
	}
}

func TestCheckAccessDeniesUnknownTeamKeys(t *testing.T) {
	srv := (&fakeLinear{teams: `[{"key":"RES"}]`}).server(t)
	resp, err := srv.CheckAccess(context.Background(), &pluginv1.CheckAccessRequest{Credentials: []*pluginv1.Credential{cred("at-1", "org-1")}, Items: []string{"RES", "ORC"}})
	if err != nil || strings.Join(resp.GetDenied(), ",") != "ORC" {
		t.Fatalf("resp=%v err=%v", resp, err)
	}
}

func TestStreamWithoutCredentialIsInvalidArgument(t *testing.T) {
	srv := (&fakeLinear{}).server(t)
	err := srv.StreamEvents(&pluginv1.StreamEventsRequest{ProjectKeys: []string{"ORC"}}, &recvStream{ctx: context.Background(), got: make(chan *pluginv1.TrackerEvent, 1)})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v", err)
	}
}

func TestCallsWithMalformedCredentialAreInvalidArgument(t *testing.T) {
	srv := (&fakeLinear{}).server(t)
	bad := &pluginv1.Credential{Data: []byte("{not json"), RefreshAfter: timestamppb.New(issued)}
	_, getErr := srv.GetTicket(context.Background(), &pluginv1.GetTicketRequest{TicketRef: "ORC-1", Credential: bad})
	_, postErr := srv.PostComment(context.Background(), &pluginv1.PostCommentRequest{TicketRef: "ORC-1", Body: "x"})
	_, checkErr := srv.CheckAccess(context.Background(), &pluginv1.CheckAccessRequest{Items: []string{"ORC"}})
	for _, err := range []error{getErr, postErr, checkErr} {
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("err = %v", err)
		}
	}
}

func TestRequestsUseBearerAccessToken(t *testing.T) {
	f := &fakeLinear{}
	srv := f.server(t)
	if _, err := srv.GetTicket(context.Background(), &pluginv1.GetTicketRequest{TicketRef: "ORC-1", Credential: cred("at-1", "org-1")}); err != nil {
		t.Fatal(err)
	}
	if len(f.auths) != 1 || f.auths[0] != "Bearer at-1" {
		t.Fatalf("auths = %v", f.auths)
	}
}

func TestDisconnectIgnoresRevokeFailure(t *testing.T) {
	srv := (&fakeLinear{}).server(t) // /oauth/revoke answers 500
	if _, err := srv.Disconnect(context.Background(), &pluginv1.DisconnectRequest{Credential: cred("at-1", "org-1")}); err != nil {
		t.Fatal(err)
	}
}

func TestSetStatusUnknownStateIsFailedPrecondition(t *testing.T) {
	srv := (&fakeLinear{}).server(t)
	_, err := srv.SetStatus(context.Background(), &pluginv1.SetStatusRequest{TicketRef: "ORC-1", Status: "Nope", Credential: cred("at-1", "org-1")})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "status Nope not found for ORC-1") {
		t.Fatalf("err = %v", err)
	}
}

func TestSetStatusMalformedCredentialIsInvalidArgument(t *testing.T) {
	srv := (&fakeLinear{}).server(t)
	_, err := srv.SetStatus(context.Background(), &pluginv1.SetStatusRequest{TicketRef: "ORC-1", Status: "Done"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v", err)
	}
}

func TestCapabilitiesSupportStatusUpdate(t *testing.T) {
	caps, err := (&fakeLinear{}).server(t).GetCapabilities(context.Background(), &pluginv1.GetCapabilitiesRequest{})
	if err != nil || !caps.GetSupportsStatusUpdate() || caps.GetAdapterName() != "linear" {
		t.Fatalf("caps=%v err=%v", caps, err)
	}
}

func TestSetStatusUnauthorizedIsUnauthenticated(t *testing.T) {
	lin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "expired", http.StatusUnauthorized)
	}))
	defer lin.Close()
	srv := newServer(lin.URL, NewHub(), false, func() time.Time { return issued })
	defer srv.Close()
	_, err := srv.SetStatus(context.Background(), &pluginv1.SetStatusRequest{TicketRef: "ORC-1", Status: "Done", Credential: cred("at-1", "org-1")})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("err = %v", err)
	}
}

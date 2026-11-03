package linear

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClientSendsRawAPIKey(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		w.Write([]byte(`{"data":{"viewer":{"id":"u1","name":"orchivio","displayName":"orchivio","email":"o@x"}}}`))
	}))
	defer srv.Close()
	v, err := New(srv.URL, "lin_api_key").Viewer(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != "lin_api_key" {
		t.Fatalf("Authorization = %q", got)
	}
	if v.ID != "u1" {
		t.Fatalf("viewer = %+v", v)
	}
}

func TestClientGraphQLErrorIsGoError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"errors":[{"message":"boom"}]}`))
	}))
	defer srv.Close()
	_, err := New(srv.URL, "secret-key").Viewer(context.Background())
	if err == nil || !strings.Contains(err.Error(), "boom") || strings.Contains(err.Error(), "secret-key") {
		t.Fatalf("err = %v", err)
	}
}

func TestClientHTTPErrorHasStatusOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal details", http.StatusBadGateway)
	}))
	defer srv.Close()
	_, err := New(srv.URL, "k").Viewer(context.Background())
	if err == nil || !strings.Contains(err.Error(), "502") || strings.Contains(err.Error(), "internal details") {
		t.Fatalf("err = %v", err)
	}
}

func TestClientPaginatesUpToCap(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Write([]byte(`{"data":{"comments":{"nodes":[{"id":"c"}],"pageInfo":{"hasNextPage":true,"endCursor":"x"}}}}`))
	}))
	defer srv.Close()
	cs, truncated, err := New(srv.URL, "k").CommentsSince(context.Background(), []string{"ORC"}, testTime)
	if err != nil {
		t.Fatal(err)
	}
	if calls != maxPages || len(cs) != maxPages || !truncated {
		t.Fatalf("calls=%d comments=%d", calls, len(cs))
	}
}

func TestCreateCommentResolvesUUIDOnceAndCaches(t *testing.T) {
	var lookups int
	var issueIDs []any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		if strings.Contains(req.Query, "commentCreate") {
			issueIDs = append(issueIDs, req.Variables["issueId"])
			w.Write([]byte(`{"data":{"commentCreate":{"success":true,"comment":{"id":"cm1"}}}}`))
			return
		}
		lookups++
		w.Write([]byte(`{"data":{"issue":{"id":"uuid-1","identifier":"ORC-1","labels":{"nodes":[]}}}}`))
	}))
	defer srv.Close()
	c := New(srv.URL, "k")
	for range 2 {
		if id, err := c.CreateComment(context.Background(), "ORC-1", "hi"); err != nil || id != "cm1" {
			t.Fatalf("id=%q err=%v", id, err)
		}
	}
	if lookups != 1 || issueIDs[0] != "uuid-1" || issueIDs[1] != "uuid-1" {
		t.Fatalf("lookups=%d ids=%v", lookups, issueIDs)
	}
}

func TestIssueMapsLabelNames(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"data":{"issue":{"id":"u","identifier":"ORC-2","title":"T","description":"D","url":"http://l/ORC-2","labels":{"nodes":[{"name":"bug"},{"name":"orchivio"}]}}}}`))
	}))
	defer srv.Close()
	is, err := New(srv.URL, "k").Issue(context.Background(), "ORC-2")
	if err != nil || strings.Join(is.Labels, ",") != "bug,orchivio" || is.Title != "T" {
		t.Fatalf("%+v %v", is, err)
	}
}

package linear

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClientSendsBearerAccessToken(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		w.Write([]byte(`{"data":{"viewer":{"id":"u1","name":"orchivio","displayName":"orchivio","email":"o@x"}}}`))
	}))
	defer srv.Close()
	v, err := New(srv.URL, "at-1").Viewer(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != "Bearer at-1" {
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

func TestClient401IsUnauthenticated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "expired", http.StatusUnauthorized)
	}))
	defer srv.Close()
	_, err := New(srv.URL, "at-1").Viewer(context.Background())
	if !errors.Is(err, ErrUnauthenticated) || strings.Contains(err.Error(), "at-1") {
		t.Fatalf("err = %v", err)
	}
}

func TestTeamKeysListsAccessibleTeams(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"data":{"teams":{"nodes":[{"key":"RES"},{"key":"ORC"}],"pageInfo":{"hasNextPage":false}}}}`))
	}))
	defer srv.Close()
	keys, err := New(srv.URL, "at-1").TeamKeys(context.Background())
	if err != nil || strings.Join(keys, ",") != "RES,ORC" {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
}

// statusServer answers issue, workflowStates and issueUpdate; mutations collects issueUpdate variables.
func statusServer(t *testing.T, currentState string, mutations *[]map[string]any) *Client {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		switch {
		case strings.Contains(req.Query, "issueUpdate"):
			*mutations = append(*mutations, req.Variables)
			w.Write([]byte(`{"data":{"issueUpdate":{"success":true}}}`))
		case strings.Contains(req.Query, "workflowStates"):
			w.Write([]byte(`{"data":{"workflowStates":{"nodes":[{"id":"s-todo","name":"Todo"},{"id":"s-prog","name":"In Progress"}],"pageInfo":{"hasNextPage":false}}}}`))
		default:
			w.Write([]byte(`{"data":{"issue":{"id":"uuid-1","identifier":"ORC-1","labels":{"nodes":[]},"state":{"name":"` + currentState + `"},"team":{"id":"team-1"}}}}`))
		}
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, "k")
}

func TestSetStatusMovesIssue(t *testing.T) {
	var mutations []map[string]any
	changed, err := statusServer(t, "Todo", &mutations).SetStatus(context.Background(), "ORC-1", "in progress")
	if err != nil || !changed {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
	if len(mutations) != 1 || mutations[0]["id"] != "uuid-1" || mutations[0]["stateId"] != "s-prog" {
		t.Fatalf("mutations = %v", mutations)
	}
}

func TestSetStatusAlreadyThere(t *testing.T) {
	var mutations []map[string]any
	changed, err := statusServer(t, "In Progress", &mutations).SetStatus(context.Background(), "ORC-1", "IN PROGRESS")
	if err != nil || changed || len(mutations) != 0 {
		t.Fatalf("changed=%v err=%v mutations=%v", changed, err, mutations)
	}
}

func TestSetStatusUnknownState(t *testing.T) {
	var mutations []map[string]any
	_, err := statusServer(t, "Todo", &mutations).SetStatus(context.Background(), "ORC-1", "Shipped")
	if !errors.Is(err, ErrStatusNotFound) || !strings.Contains(err.Error(), "status Shipped not found for ORC-1") || len(mutations) != 0 {
		t.Fatalf("err=%v mutations=%v", err, mutations)
	}
}

func TestSetStatusMissingIssueIsNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"data":{"issue":null}}`))
	}))
	defer srv.Close()
	_, err := New(srv.URL, "k").SetStatus(context.Background(), "ORC-9", "Done")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestSetStatusUnsuccessfulMutationIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Query string }
		json.NewDecoder(r.Body).Decode(&req)
		switch {
		case strings.Contains(req.Query, "issueUpdate"):
			w.Write([]byte(`{"data":{"issueUpdate":{"success":false}}}`))
		case strings.Contains(req.Query, "workflowStates"):
			w.Write([]byte(`{"data":{"workflowStates":{"nodes":[{"id":"s1","name":"Done"}],"pageInfo":{"hasNextPage":false}}}}`))
		default:
			w.Write([]byte(`{"data":{"issue":{"id":"u","state":{"name":"Todo"},"team":{"id":"t"}}}}`))
		}
	}))
	defer srv.Close()
	changed, err := New(srv.URL, "k").SetStatus(context.Background(), "ORC-1", "Done")
	if err == nil || changed || !strings.Contains(err.Error(), "not successful") {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
}

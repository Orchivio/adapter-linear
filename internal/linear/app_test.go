package linear

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

const viewerBody = `{"data":{"viewer":{"id":"app1","name":"Orchivio","displayName":"orchivio","email":""}}}`

// tokenServer issues tok1, tok2, ... on each client_credentials request.
func tokenServer(t *testing.T, expiresIn int) (*httptest.Server, *int) {
	issued := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, secret, ok := r.BasicAuth()
		r.ParseForm()
		if !ok || id != "cid" || secret != "csecret" || r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("scope") != AppScopes {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		issued++
		w.Write([]byte(`{"access_token":"tok` + strconv.Itoa(issued) + `","token_type":"Bearer","expires_in":` + strconv.Itoa(expiresIn) + `}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &issued
}

func TestAppClientSendsBearerTokenAndCachesIt(t *testing.T) {
	tokens, issued := tokenServer(t, 2591999)
	var auths []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auths = append(auths, r.Header.Get("Authorization"))
		w.Write([]byte(`{"data":{"issue":{"id":"u","identifier":"ORC-1","labels":{"nodes":[]}}}}`))
	}))
	defer api.Close()
	c := NewApp(api.URL, tokens.URL, "cid", "csecret")
	for range 2 {
		if _, err := c.Issue(context.Background(), "ORC-1"); err != nil {
			t.Fatal(err)
		}
	}
	if *issued != 1 || auths[0] != "Bearer tok1" || auths[1] != "Bearer tok1" {
		t.Fatalf("issued=%d auths=%v", *issued, auths)
	}
}

func TestAppClientRenewsTokenOn401(t *testing.T) {
	tokens, issued := tokenServer(t, 2591999)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer tok1" {
			http.Error(w, "expired", http.StatusUnauthorized)
			return
		}
		w.Write([]byte(viewerBody))
	}))
	defer api.Close()
	v, err := NewApp(api.URL, tokens.URL, "cid", "csecret").Viewer(context.Background())
	if err != nil || v.ID != "app1" || *issued != 2 {
		t.Fatalf("v=%+v err=%v issued=%d", v, err, *issued)
	}
}

func TestAppClientRenewsTokenBeforeExpiry(t *testing.T) {
	tokens, issued := tokenServer(t, 60) // inside the renewal margin
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"data":{"issue":{"id":"u","identifier":"ORC-1","labels":{"nodes":[]}}}}`))
	}))
	defer api.Close()
	c := NewApp(api.URL, tokens.URL, "cid", "csecret")
	c.Issue(context.Background(), "ORC-1")
	c.Issue(context.Background(), "ORC-1")
	if *issued != 2 {
		t.Fatalf("issued=%d, want a fresh token per call inside the margin", *issued)
	}
}

func TestAppClientTokenErrorHidesSecret(t *testing.T) {
	tokens := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error_description":"Client does not support the client_credentials grant type"}`, http.StatusBadRequest)
	}))
	defer tokens.Close()
	_, err := NewApp("http://unused", tokens.URL, "cid", "csecret").Viewer(context.Background())
	if err == nil || !strings.Contains(err.Error(), "400") || strings.Contains(err.Error(), "csecret") {
		t.Fatalf("err = %v", err)
	}
}

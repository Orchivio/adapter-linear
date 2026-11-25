package linear

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// fakeLinear serves /oauth/token, /oauth/revoke and /graphql; token answers
// the token endpoint, forms records every token/revoke request.
type fakeLinear struct {
	token  func(w http.ResponseWriter, form url.Values)
	forms  []url.Values
	auths  []string
	revoke int
}

func (f *fakeLinear) oauth(t *testing.T) *OAuth {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		switch r.URL.Path {
		case "/oauth/token":
			f.forms = append(f.forms, r.PostForm)
			f.token(w, r.PostForm)
		case "/oauth/revoke":
			f.forms = append(f.forms, r.PostForm)
			w.WriteHeader(f.revoke)
		case "/graphql":
			f.auths = append(f.auths, r.Header.Get("Authorization"))
			w.Write([]byte(`{"data":{"organization":{"id":"org-1","name":"Cubie Studio"}}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return &OAuth{APIURL: srv.URL, OAuthURL: "https://linear.example", ClientID: "cid", ClientSecret: "csecret", Now: func() time.Time { return testTime }}
}

func tokenJSON(w http.ResponseWriter, _ url.Values) {
	w.Write([]byte(`{"access_token":"at-1","refresh_token":"rt-1","token_type":"Bearer","expires_in":86399}`))
}

func TestAuthorizeURLAsksForAppActor(t *testing.T) {
	u, err := url.Parse((&OAuth{OAuthURL: "https://linear.example", ClientID: "cid"}).AuthorizeURL("https://o/cb", "st"))
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if u.Scheme+"://"+u.Host+u.Path != "https://linear.example/oauth/authorize" || q.Get("client_id") != "cid" ||
		q.Get("redirect_uri") != "https://o/cb" || q.Get("response_type") != "code" || q.Get("scope") != Scopes ||
		q.Get("actor") != "app" || q.Get("state") != "st" || q.Get("prompt") != "consent" {
		t.Fatalf("url = %s", u)
	}
}

func TestExchangeReturnsWorkspaceCredential(t *testing.T) {
	f := &fakeLinear{token: tokenJSON}
	cred, err := f.oauth(t).Exchange(context.Background(), "https://o/cb", "code-1")
	if err != nil {
		t.Fatal(err)
	}
	want := Credential{AccessToken: "at-1", RefreshToken: "rt-1", ExpiresAt: testTime.Add(86399 * time.Second), OrganizationID: "org-1", OrganizationName: "Cubie Studio"}
	if cred != want {
		t.Fatalf("cred = %+v", cred)
	}
	form := f.forms[0]
	if form.Get("grant_type") != "authorization_code" || form.Get("code") != "code-1" || form.Get("redirect_uri") != "https://o/cb" ||
		form.Get("client_id") != "cid" || form.Get("client_secret") != "csecret" {
		t.Fatalf("form = %v", form)
	}
	if f.auths[0] != "Bearer at-1" {
		t.Fatalf("graphql auth = %q", f.auths[0])
	}
}

func TestRefreshKeepsWorkspaceAndRotatesTokens(t *testing.T) {
	f := &fakeLinear{token: func(w http.ResponseWriter, _ url.Values) {
		w.Write([]byte(`{"access_token":"at-2","refresh_token":"rt-2","expires_in":100}`))
	}}
	old := Credential{AccessToken: "at-1", RefreshToken: "rt-1", OrganizationID: "org-1", OrganizationName: "Cubie Studio"}
	cred, err := f.oauth(t).Refresh(context.Background(), old)
	if err != nil {
		t.Fatal(err)
	}
	want := Credential{AccessToken: "at-2", RefreshToken: "rt-2", ExpiresAt: testTime.Add(100 * time.Second), OrganizationID: "org-1", OrganizationName: "Cubie Studio"}
	if cred != want {
		t.Fatalf("cred = %+v", cred)
	}
	if form := f.forms[0]; form.Get("grant_type") != "refresh_token" || form.Get("refresh_token") != "rt-1" || form.Get("client_secret") != "csecret" {
		t.Fatalf("form = %v", form)
	}
}

func TestRefreshErrorsAreClassifiedWithoutSecrets(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		unauth bool
	}{
		{"invalid grant", http.StatusBadRequest, `{"error":"invalid_grant","error_description":"rt-1 revoked"}`, true},
		{"unauthorized", http.StatusUnauthorized, `{"error":"invalid_client"}`, true},
		{"server error", http.StatusBadGateway, `rt-1 csecret`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeLinear{token: func(w http.ResponseWriter, _ url.Values) { http.Error(w, tc.body, tc.status) }}
			_, err := f.oauth(t).Refresh(context.Background(), Credential{AccessToken: "at-1", RefreshToken: "rt-1"})
			if err == nil || errors.Is(err, ErrUnauthenticated) != tc.unauth {
				t.Fatalf("err = %v, want unauthenticated=%v", err, tc.unauth)
			}
			for _, secret := range []string{"rt-1", "at-1", "csecret"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("err leaks %q: %v", secret, err)
				}
			}
		})
	}
}

func TestRevokeSendsAccessToken(t *testing.T) {
	f := &fakeLinear{revoke: http.StatusOK}
	if err := f.oauth(t).Revoke(context.Background(), "at-1"); err != nil {
		t.Fatal(err)
	}
	if f.forms[0].Get("token") != "at-1" {
		t.Fatalf("form = %v", f.forms[0])
	}
}

func TestRevokeErrorHidesToken(t *testing.T) {
	f := &fakeLinear{revoke: http.StatusBadRequest}
	err := f.oauth(t).Revoke(context.Background(), "at-1")
	if err == nil || strings.Contains(err.Error(), "at-1") {
		t.Fatalf("err = %v", err)
	}
}

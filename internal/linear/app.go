package linear

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultTokenURL is Linear's OAuth token endpoint.
	DefaultTokenURL = "https://api.linear.app/oauth/token"
	// AppScopes lets the app user read, comment, be mentioned and be delegated issues.
	AppScopes = "read,write,app:assignable,app:mentionable"
	// renewBefore fetches a new token this long before the old one expires.
	renewBefore = 24 * time.Hour
)

// appToken fetches client_credentials tokens (actor=app, 30 days, no refresh
// token) and caches one until shortly before it expires.
type appToken struct {
	tokenURL, clientID, clientSecret string
	http                             *http.Client
	now                              func() time.Time

	mu      sync.Mutex
	token   string
	expires time.Time
}

// header returns the Authorization value; renew drops the cached token first.
func (a *appToken) header(ctx context.Context, renew bool) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if renew || a.token == "" || a.now().After(a.expires.Add(-renewBefore)) {
		if err := a.fetch(ctx); err != nil {
			return "", err
		}
	}
	return "Bearer " + a.token, nil
}

func (a *appToken) fetch(ctx context.Context) error {
	form := url.Values{"grant_type": {"client_credentials"}, "scope": {AppScopes}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("linear: build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(a.clientID, a.clientSecret)
	resp, err := a.http.Do(req)
	if err != nil {
		return fmt.Errorf("linear: token request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// Body may echo request details; report the status only.
		return fmt.Errorf("linear: token request http status %d (client credentials enabled on the OAuth app?)", resp.StatusCode)
	}
	var tok struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil || tok.AccessToken == "" {
		return fmt.Errorf("linear: token response without access_token")
	}
	a.token, a.expires = tok.AccessToken, a.now().Add(time.Duration(tok.ExpiresIn)*time.Second)
	return nil
}

// NewApp creates a client acting as the OAuth app user via client_credentials.
func NewApp(endpoint, tokenURL, clientID, clientSecret string) *Client {
	c := newClient(endpoint)
	t := &appToken{tokenURL: tokenURL, clientID: clientID, clientSecret: clientSecret, http: c.http, now: time.Now}
	c.auth = t.header
	return c
}

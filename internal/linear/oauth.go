package linear

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// DefaultAPIURL hosts GraphQL (/graphql) and the token endpoints (/oauth/...).
	DefaultAPIURL = "https://api.linear.app"
	// DefaultOAuthURL hosts the consent page (/oauth/authorize).
	DefaultOAuthURL = "https://linear.app"
	// Scopes lets the app user read, comment, be mentioned and be delegated issues.
	Scopes = "read,write,app:assignable,app:mentionable"
)

// Credential is one workspace's access record. The orchestrator stores it
// encrypted and passes it on every call; the adapter keeps none.
type Credential struct {
	AccessToken      string    `json:"accessToken"`
	RefreshToken     string    `json:"refreshToken"`
	ExpiresAt        time.Time `json:"expiresAt"`
	OrganizationID   string    `json:"organizationId"`
	OrganizationName string    `json:"organizationName"`
}

// OAuth is the installation's Linear OAuth app (actor=app).
type OAuth struct {
	APIURL, OAuthURL       string
	ClientID, ClientSecret string
	Now                    func() time.Time
}

// AuthorizeURL is the consent page that installs the app into a workspace.
func (o *OAuth) AuthorizeURL(redirectURI, state string) string {
	q := url.Values{
		"client_id":     {o.ClientID},
		"redirect_uri":  {redirectURI},
		"response_type": {"code"},
		"scope":         {Scopes},
		"actor":         {"app"},
		"state":         {state},
		"prompt":        {"consent"},
	}
	return o.OAuthURL + "/oauth/authorize?" + q.Encode()
}

// Client returns a GraphQL client acting with accessToken.
func (o *OAuth) Client(accessToken string) *Client {
	return New(o.APIURL+"/graphql", accessToken)
}

// Exchange trades an authorization code for tokens and reads the workspace.
func (o *OAuth) Exchange(ctx context.Context, redirectURI, code string) (Credential, error) {
	cred, err := o.token(ctx, url.Values{"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {redirectURI}})
	if err != nil {
		return Credential{}, err
	}
	cred.OrganizationID, cred.OrganizationName, err = o.Client(cred.AccessToken).Organization(ctx)
	if err != nil {
		return Credential{}, err
	}
	return cred, nil
}

// Refresh renews the tokens of cred; the workspace stays the same.
func (o *OAuth) Refresh(ctx context.Context, cred Credential) (Credential, error) {
	next, err := o.token(ctx, url.Values{"grant_type": {"refresh_token"}, "refresh_token": {cred.RefreshToken}})
	if err != nil {
		return Credential{}, err
	}
	if next.RefreshToken == "" { // not rotated
		next.RefreshToken = cred.RefreshToken
	}
	next.OrganizationID, next.OrganizationName = cred.OrganizationID, cred.OrganizationName
	return next, nil
}

// Revoke invalidates accessToken (and its grant) at Linear.
func (o *OAuth) Revoke(ctx context.Context, accessToken string) error {
	resp, err := o.post(ctx, "/oauth/revoke", url.Values{"token": {accessToken}})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("linear: revoke http status %d", resp.StatusCode)
	}
	return nil
}

// token calls the token endpoint. Errors carry the HTTP status and OAuth error
// code only: the body may echo the grant.
func (o *OAuth) token(ctx context.Context, form url.Values) (Credential, error) {
	form.Set("client_id", o.ClientID)
	form.Set("client_secret", o.ClientSecret)
	issued := o.Now()
	resp, err := o.post(ctx, "/oauth/token", form)
	if err != nil {
		return Credential{}, err
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
		Error        string `json:"error"`
	}
	decodeErr := json.NewDecoder(resp.Body).Decode(&body)
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusUnauthorized || body.Error == "invalid_grant" {
			return Credential{}, fmt.Errorf("%w (token http %d %s)", ErrUnauthenticated, resp.StatusCode, oauthCode(body.Error))
		}
		return Credential{}, fmt.Errorf("linear: token http status %d %s", resp.StatusCode, oauthCode(body.Error))
	}
	if decodeErr != nil || body.AccessToken == "" {
		return Credential{}, errors.New("linear: token response without access_token")
	}
	return Credential{AccessToken: body.AccessToken, RefreshToken: body.RefreshToken, ExpiresAt: issued.Add(time.Duration(body.ExpiresIn) * time.Second)}, nil
}

// oauthCode keeps only a well-formed OAuth error code (RFC 6749: [a-z_]).
func oauthCode(code string) string {
	if code == "" || strings.Trim(code, "abcdefghijklmnopqrstuvwxyz_") != "" {
		return ""
	}
	return code
}

func (o *OAuth) post(ctx context.Context, path string, form url.Values) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.APIURL+path, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, errors.New("linear: build oauth request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := httpClient.Do(req)
	if err != nil {
		// url.Error names the URL only (no form values); unwrap to the cause anyway.
		return nil, fmt.Errorf("linear: oauth request failed: %w", errors.Unwrap(err))
	}
	return resp, nil
}

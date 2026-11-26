// Package linear is a minimal Linear GraphQL client (net/http only).
package linear

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ErrUnauthenticated marks a revoked or expired access token or refresh grant.
var ErrUnauthenticated = errors.New("linear: access token or grant no longer valid")

// ErrStatusNotFound marks a status name the ticket's team has no workflow state for.
var ErrStatusNotFound = errors.New("linear: status not found")

// ErrNotFound marks a ticket that does not exist or is not visible to the app.
var ErrNotFound = errors.New("linear: not found")

const (
	pageSize       = 100
	maxPages       = 5 // ponytail: cap per poll; add continuation if a team exceeds 500 changes per interval
	requestTimeout = 30 * time.Second
)

// Client talks to Linear as one workspace's app user. Safe for concurrent use.
type Client struct {
	endpoint string
	token    string // OAuth access token, sent as Bearer
	http     *http.Client

	mu     sync.Mutex
	viewer *Viewer
	ids    map[string]string // identifier -> issue UUID
}

// httpClient is shared so connections are reused across per-call clients.
var httpClient = &http.Client{Timeout: requestTimeout}

// New creates a client for the GraphQL endpoint and an OAuth access token.
func New(endpoint, accessToken string) *Client {
	return &Client{endpoint: endpoint, token: accessToken, http: httpClient, ids: map[string]string{}}
}

type Viewer struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
	Email       string `json:"email"`
}

type User struct {
	ID          string `json:"id"`
	Email       string `json:"email"`
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
}

type HistoryEntry struct {
	CreatedAt  time.Time `json:"createdAt"`
	Actor      *User     `json:"actor"`
	ToAssignee *struct {
		ID string `json:"id"`
	} `json:"toAssignee"`
	// ToDelegate is set when an issue is delegated to an app user.
	ToDelegate *struct {
		ID string `json:"id"`
	} `json:"toDelegate"`
}

type Assignment struct {
	IssueID    string    `json:"id"`
	Identifier string    `json:"identifier"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
	Creator    *User     `json:"creator"`
	Assignee   *struct {
		ID string `json:"id"`
	} `json:"assignee"`
	Delegate *struct {
		ID string `json:"id"`
	} `json:"delegate"`
	Team struct {
		Key string `json:"key"`
	} `json:"team"`
	History struct {
		Nodes []HistoryEntry `json:"nodes"`
	} `json:"history"`
}

type Comment struct {
	ID        string    `json:"id"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"createdAt"`
	User      *User     `json:"user"` // nil for bot/integration comments
	Issue     struct {
		Identifier string `json:"identifier"`
		Team       struct {
			Key string `json:"key"`
		} `json:"team"`
	} `json:"issue"`
}

type Issue struct {
	Identifier  string
	Title       string
	Description string
	URL         string
	Labels      []string
}

const (
	viewerQuery = `query { viewer { id name displayName email } }`

	assignedQuery = `query($teams: [String!], $since: DateTimeOrDuration!, $after: String) {
  issues(first: 100, after: $after, filter: { or: [{ assignee: { isMe: { eq: true } } }, { delegate: { isMe: { eq: true } } }], updatedAt: { gt: $since }, team: { key: { in: $teams } } }) {
    nodes { id identifier createdAt updatedAt creator { id email name displayName } assignee { id } delegate { id } team { key } history(first: 50) { nodes { createdAt actor { id email name displayName } toAssignee { id } toDelegate { id } } } }
    pageInfo { hasNextPage endCursor }
  }
}`

	commentsQuery = `query($teams: [String!], $since: DateTimeOrDuration!, $after: String) {
  comments(first: 100, after: $after, filter: { createdAt: { gt: $since }, issue: { team: { key: { in: $teams } } } }) {
    nodes { id body createdAt user { id email name displayName } issue { identifier team { key } } }
    pageInfo { hasNextPage endCursor }
  }
}`

	issueQuery = `query($ref: String!) { issue(id: $ref) { id identifier title description url labels { nodes { name } } } }`

	issueStateQuery = `query($ref: String!) { issue(id: $ref) { id state { name } team { id } } }`

	workflowStatesQuery = `query($team: ID!, $after: String) { workflowStates(first: 100, after: $after, filter: { team: { id: { eq: $team } } }) { nodes { id name } pageInfo { hasNextPage endCursor } } }`

	issueUpdateMutation = `mutation($id: String!, $stateId: String!) { issueUpdate(id: $id, input: { stateId: $stateId }) { success } }`

	organizationQuery = `query { organization { id name } }`

	teamsQuery = `query($after: String) { teams(first: 100, after: $after) { nodes { key } pageInfo { hasNextPage endCursor } } }`

	commentCreateMutation = `mutation($issueId: String!, $body: String!) { commentCreate(input: { issueId: $issueId, body: $body }) { success comment { id } } }`
)

func (c *Client) do(ctx context.Context, query string, vars map[string]any, out any) error {
	payload, err := json.Marshal(map[string]any{"query": query, "variables": vars})
	if err != nil {
		return fmt.Errorf("linear: encode request: %w", err)
	}
	resp, err := c.post(ctx, payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("%w (http 401)", ErrUnauthenticated)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("linear: http status %d", resp.StatusCode)
	}
	var env struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return fmt.Errorf("linear: decode response: %w", err)
	}
	if len(env.Errors) > 0 {
		msgs := make([]string, len(env.Errors))
		for i, e := range env.Errors {
			msgs[i] = e.Message
		}
		return fmt.Errorf("linear: graphql error: %s", strings.Join(msgs, "; "))
	}
	if err := json.Unmarshal(env.Data, out); err != nil {
		return fmt.Errorf("linear: decode data: %w", err)
	}
	return nil
}

func (c *Client) post(ctx context.Context, payload []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("linear: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		// url.Error carries the URL only, never headers.
		return nil, fmt.Errorf("linear: request failed: %w", err)
	}
	return resp, nil
}

// Viewer returns the authenticated user (or app user); fetched once and cached.
func (c *Client) Viewer(ctx context.Context) (Viewer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.viewer != nil {
		return *c.viewer, nil
	}
	var d struct {
		Viewer Viewer `json:"viewer"`
	}
	if err := c.do(ctx, viewerQuery, nil, &d); err != nil {
		return Viewer{}, err
	}
	c.viewer = &d.Viewer
	return d.Viewer, nil
}

type pageInfo struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

// paginate runs query up to maxPages times; field is the connection name.
// truncated is true when the cap was hit while hasNextPage was still true.
func paginate[T any](ctx context.Context, c *Client, query, field string, vars map[string]any) (all []T, truncated bool, err error) {
	for page := 0; page < maxPages; page++ {
		var d map[string]struct {
			Nodes    []T      `json:"nodes"`
			PageInfo pageInfo `json:"pageInfo"`
		}
		if err := c.do(ctx, query, vars, &d); err != nil {
			return nil, false, err
		}
		conn := d[field]
		all = append(all, conn.Nodes...)
		if !conn.PageInfo.HasNextPage {
			return all, false, nil
		}
		vars["after"] = conn.PageInfo.EndCursor
	}
	return all, true, nil
}

// AssignedSince returns issues assigned to the viewer and updated after since.
// truncated reports that the page cap was reached with more results pending.
func (c *Client) AssignedSince(ctx context.Context, teams []string, since time.Time) ([]Assignment, bool, error) {
	return paginate[Assignment](ctx, c, assignedQuery, "issues", map[string]any{"teams": teams, "since": since.UTC().Format(time.RFC3339Nano)})
}

// CommentsSince returns comments created after since on the given teams.
// truncated reports that the page cap was reached with more results pending.
func (c *Client) CommentsSince(ctx context.Context, teams []string, since time.Time) ([]Comment, bool, error) {
	return paginate[Comment](ctx, c, commentsQuery, "comments", map[string]any{"teams": teams, "since": since.UTC().Format(time.RFC3339Nano)})
}

type issueNode struct {
	ID          string `json:"id"`
	Identifier  string `json:"identifier"`
	Title       string `json:"title"`
	Description string `json:"description"`
	URL         string `json:"url"`
	Labels      struct {
		Nodes []struct {
			Name string `json:"name"`
		} `json:"nodes"`
	} `json:"labels"`
}

// Issue reads a ticket by identifier (e.g. "ORC-12").
func (c *Client) Issue(ctx context.Context, ref string) (Issue, error) {
	n, err := c.issueNode(ctx, ref)
	if err != nil {
		return Issue{}, err
	}
	is := Issue{Identifier: n.Identifier, Title: n.Title, Description: n.Description, URL: n.URL, Labels: []string{}}
	for _, l := range n.Labels.Nodes {
		is.Labels = append(is.Labels, l.Name)
	}
	return is, nil
}

func (c *Client) issueNode(ctx context.Context, ref string) (issueNode, error) {
	var d struct {
		Issue *issueNode `json:"issue"`
	}
	if err := c.do(ctx, issueQuery, map[string]any{"ref": ref}, &d); err != nil {
		return issueNode{}, err
	}
	if d.Issue == nil {
		return issueNode{}, fmt.Errorf("%w: issue %q", ErrNotFound, ref)
	}
	c.mu.Lock()
	c.ids[ref] = d.Issue.ID
	c.mu.Unlock()
	return *d.Issue, nil
}

// CreateComment posts a comment; commentCreate needs the UUID, so the
// identifier is resolved first and cached per process.
func (c *Client) CreateComment(ctx context.Context, ref, body string) (string, error) {
	c.mu.Lock()
	id, ok := c.ids[ref]
	c.mu.Unlock()
	if !ok {
		n, err := c.issueNode(ctx, ref)
		if err != nil {
			return "", err
		}
		id = n.ID
	}
	var d struct {
		CommentCreate struct {
			Success bool `json:"success"`
			Comment *struct {
				ID string `json:"id"`
			} `json:"comment"`
		} `json:"commentCreate"`
	}
	if err := c.do(ctx, commentCreateMutation, map[string]any{"issueId": id, "body": body}, &d); err != nil {
		return "", err
	}
	if !d.CommentCreate.Success || d.CommentCreate.Comment == nil {
		return "", fmt.Errorf("linear: commentCreate on %q was not successful", ref)
	}
	return d.CommentCreate.Comment.ID, nil
}

// Organization returns the workspace the access token belongs to.
func (c *Client) Organization(ctx context.Context) (id, name string, err error) {
	var d struct {
		Organization struct{ ID, Name string } `json:"organization"`
	}
	if err := c.do(ctx, organizationQuery, nil, &d); err != nil {
		return "", "", err
	}
	if d.Organization.ID == "" {
		return "", "", errors.New("linear: organization without id")
	}
	return d.Organization.ID, d.Organization.Name, nil
}

// TeamKeys lists the keys of all teams the app user can access.
// ponytail: capped at maxPages*100 teams; keys beyond the cap read as denied.
func (c *Client) TeamKeys(ctx context.Context) ([]string, error) {
	teams, _, err := paginate[struct{ Key string }](ctx, c, teamsQuery, "teams", map[string]any{})
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(teams))
	for i, t := range teams {
		keys[i] = t.Key
	}
	return keys, nil
}

// SetStatus moves the ticket to the team workflow state named status (case-insensitive).
// It reports false without a mutation when the ticket already is in that state.
func (c *Client) SetStatus(ctx context.Context, ref, status string) (bool, error) {
	var d struct {
		Issue *struct {
			ID    string `json:"id"`
			State struct {
				Name string `json:"name"`
			} `json:"state"`
			Team struct {
				ID string `json:"id"`
			} `json:"team"`
		} `json:"issue"`
	}
	if err := c.do(ctx, issueStateQuery, map[string]any{"ref": ref}, &d); err != nil {
		return false, err
	}
	if d.Issue == nil {
		return false, fmt.Errorf("%w: issue %q", ErrNotFound, ref)
	}
	if strings.EqualFold(d.Issue.State.Name, status) {
		return false, nil
	}
	states, _, err := paginate[struct{ ID, Name string }](ctx, c, workflowStatesQuery, "workflowStates", map[string]any{"team": d.Issue.Team.ID})
	if err != nil {
		return false, err
	}
	for _, st := range states {
		if !strings.EqualFold(st.Name, status) {
			continue
		}
		var u struct {
			IssueUpdate struct {
				Success bool `json:"success"`
			} `json:"issueUpdate"`
		}
		if err := c.do(ctx, issueUpdateMutation, map[string]any{"id": d.Issue.ID, "stateId": st.ID}, &u); err != nil {
			return false, err
		}
		if !u.IssueUpdate.Success {
			return false, fmt.Errorf("linear: issueUpdate on %q was not successful", ref)
		}
		return true, nil
	}
	return false, fmt.Errorf("%w: status %s not found for %s", ErrStatusNotFound, status, ref)
}

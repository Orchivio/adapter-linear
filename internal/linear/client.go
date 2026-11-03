// Package linear is a minimal Linear GraphQL client (net/http only).
package linear

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultEndpoint is Linear's public GraphQL endpoint.
	DefaultEndpoint = "https://api.linear.app/graphql"
	pageSize        = 100
	maxPages        = 5 // ponytail: cap per poll; add continuation if a team exceeds 500 changes per interval
	requestTimeout  = 30 * time.Second
)

// Client talks to Linear. Safe for concurrent use.
type Client struct {
	endpoint string
	apiKey   string
	http     *http.Client

	mu     sync.Mutex
	viewer *Viewer
	ids    map[string]string // identifier -> issue UUID
}

// New creates a client. The API key is sent raw (no "Bearer").
func New(endpoint, apiKey string) *Client {
	return &Client{endpoint: endpoint, apiKey: apiKey, http: &http.Client{Timeout: requestTimeout}, ids: map[string]string{}}
}

type Viewer struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
	Email       string `json:"email"`
}

type User struct {
	ID    string `json:"id"`
	Email string `json:"email"`
	Name  string `json:"name"`
}

type HistoryEntry struct {
	CreatedAt  time.Time `json:"createdAt"`
	Actor      *User     `json:"actor"`
	ToAssignee *struct {
		ID string `json:"id"`
	} `json:"toAssignee"`
}

type Assignment struct {
	IssueID    string    `json:"id"`
	Identifier string    `json:"identifier"`
	UpdatedAt  time.Time `json:"updatedAt"`
	Team       struct {
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
  issues(first: 100, after: $after, filter: { assignee: { isMe: { eq: true } }, updatedAt: { gt: $since }, team: { key: { in: $teams } } }) {
    nodes { id identifier updatedAt team { key } history(first: 10) { nodes { createdAt actor { id email name } toAssignee { id } } } }
    pageInfo { hasNextPage endCursor }
  }
}`

	commentsQuery = `query($teams: [String!], $since: DateTimeOrDuration!, $after: String) {
  comments(first: 100, after: $after, filter: { createdAt: { gt: $since }, issue: { team: { key: { in: $teams } } } }) {
    nodes { id body createdAt user { id email name } issue { identifier team { key } } }
    pageInfo { hasNextPage endCursor }
  }
}`

	issueQuery = `query($ref: String!) { issue(id: $ref) { id identifier title description url labels { nodes { name } } } }`

	commentCreateMutation = `mutation($issueId: String!, $body: String!) { commentCreate(input: { issueId: $issueId, body: $body }) { success comment { id } } }`
)

func (c *Client) do(ctx context.Context, query string, vars map[string]any, out any) error {
	payload, err := json.Marshal(map[string]any{"query": query, "variables": vars})
	if err != nil {
		return fmt.Errorf("linear: encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("linear: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", c.apiKey)
	resp, err := c.http.Do(req)
	if err != nil {
		// url.Error carries the URL only, never headers.
		return fmt.Errorf("linear: request failed: %w", err)
	}
	defer resp.Body.Close()
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

// Viewer returns the API key's user; fetched once and cached.
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
func paginate[T any](ctx context.Context, c *Client, query, field string, vars map[string]any) ([]T, error) {
	var all []T
	for page := 0; page < maxPages; page++ {
		var d map[string]struct {
			Nodes    []T      `json:"nodes"`
			PageInfo pageInfo `json:"pageInfo"`
		}
		if err := c.do(ctx, query, vars, &d); err != nil {
			return nil, err
		}
		conn := d[field]
		all = append(all, conn.Nodes...)
		if !conn.PageInfo.HasNextPage {
			break
		}
		vars["after"] = conn.PageInfo.EndCursor
	}
	return all, nil
}

// AssignedSince returns issues assigned to the viewer and updated after since.
func (c *Client) AssignedSince(ctx context.Context, teams []string, since time.Time) ([]Assignment, error) {
	return paginate[Assignment](ctx, c, assignedQuery, "issues", map[string]any{"teams": teams, "since": since.UTC().Format(time.RFC3339Nano)})
}

// CommentsSince returns comments created after since on the given teams.
func (c *Client) CommentsSince(ctx context.Context, teams []string, since time.Time) ([]Comment, error) {
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
		return issueNode{}, fmt.Errorf("linear: issue %q not found", ref)
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

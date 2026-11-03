// Package poller turns Linear state into normalized TrackerEvents.
package poller

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode"

	pluginv1 "github.com/soemii/orchivio-server/contract/gen/orchivio/plugin/v1"

	"github.com/soemii/orchivio-adapter-linear/internal/linear"
)

// overlap re-reads a small window so events with equal timestamps are not
// missed; the server dedups on (source, external_id).
const overlap = time.Second

// Poller holds the cursors of one event stream.
// ponytail: one poller per stream, no shared poller + fan-out; share if many subscribers appear.
type Poller struct {
	c        *linear.Client
	teams    []string
	interval time.Duration
	log      *slog.Logger

	assignedCursor time.Time
	commentCursor  time.Time
}

// New starts both cursors at now (no replay of old events).
func New(c *linear.Client, teams []string, now time.Time, interval time.Duration) *Poller {
	return &Poller{c: c, teams: teams, interval: interval, log: slog.Default(), assignedCursor: now, commentCursor: now}
}

// Poll fetches new assignments and comments since the cursors.
func (p *Poller) Poll(ctx context.Context) ([]*pluginv1.TrackerEvent, error) {
	viewer, err := p.c.Viewer(ctx)
	if err != nil {
		return nil, err
	}
	var events []*pluginv1.TrackerEvent

	assigned, err := p.c.AssignedSince(ctx, p.teams, p.assignedCursor.Add(-overlap))
	if err != nil {
		return nil, err
	}
	for _, a := range assigned {
		if a.UpdatedAt.After(p.assignedCursor) {
			p.assignedCursor = a.UpdatedAt
		}
		if ev := p.assignmentEvent(a, viewer); ev != nil {
			events = append(events, ev)
		}
	}

	comments, err := p.c.CommentsSince(ctx, p.teams, p.commentCursor.Add(-overlap))
	if err != nil {
		return nil, err
	}
	for _, cm := range comments {
		if cm.CreatedAt.After(p.commentCursor) {
			p.commentCursor = cm.CreatedAt
		}
		if cm.User == nil || cm.User.ID == viewer.ID { // bot comments and our own: no loops
			continue
		}
		events = append(events, &pluginv1.TrackerEvent{
			ExternalId:  "comment:" + cm.ID,
			Kind:        pluginv1.TrackerEvent_COMMENTED,
			TicketRef:   cm.Issue.Identifier,
			ProjectKey:  cm.Issue.Team.Key,
			Actor:       &pluginv1.Actor{Id: cm.User.ID, Email: cm.User.Email, Name: cm.User.Name},
			Body:        cm.Body,
			MentionsBot: mentions(cm.Body, viewer),
		})
	}
	return events, nil
}

func (p *Poller) assignmentEvent(a linear.Assignment, viewer linear.Viewer) *pluginv1.TrackerEvent {
	var best *linear.HistoryEntry
	for i := range a.History.Nodes {
		h := &a.History.Nodes[i]
		if h.ToAssignee != nil && h.ToAssignee.ID == viewer.ID && (best == nil || h.CreatedAt.After(best.CreatedAt)) {
			best = h
		}
	}
	if best == nil || best.Actor == nil {
		p.log.Warn("assignment without history entry, skipping", "issue", a.Identifier)
		return nil
	}
	return &pluginv1.TrackerEvent{
		ExternalId: fmt.Sprintf("assign:%s:%s", a.IssueID, best.CreatedAt.UTC().Format(time.RFC3339Nano)),
		Kind:       pluginv1.TrackerEvent_ASSIGNED,
		TicketRef:  a.Identifier,
		ProjectKey: a.Team.Key,
		Actor:      &pluginv1.Actor{Id: best.Actor.ID, Email: best.Actor.Email, Name: best.Actor.Name},
	}
}

// mentions reports an @displayName/@name token (case-insensitive, whole token)
// or a profile URL containing /profiles/<displayName>.
func mentions(body string, v linear.Viewer) bool {
	lower := strings.ToLower(body)
	for _, name := range []string{v.DisplayName, v.Name} {
		if name != "" && hasToken(lower, "@"+strings.ToLower(name)) {
			return true
		}
	}
	return v.DisplayName != "" && strings.Contains(lower, "/profiles/"+strings.ToLower(v.DisplayName))
}

func hasToken(s, tok string) bool {
	for off := 0; ; {
		i := strings.Index(s[off:], tok)
		if i < 0 {
			return false
		}
		end := off + i + len(tok)
		if end == len(s) {
			return true
		}
		if r := []rune(s[end:])[0]; !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return true
		}
		off += i + 1
	}
}

// Run polls, sends, then waits for the interval or a nudge. Poll errors are
// logged and retried on the next tick; it returns on ctx cancel or send error.
func (p *Poller) Run(ctx context.Context, nudge <-chan struct{}, send func(*pluginv1.TrackerEvent) error) error {
	for {
		events, err := p.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			p.log.Warn("poll failed", "err", err)
		}
		for _, ev := range events {
			if err := send(ev); err != nil {
				return err
			}
		}
		timer := time.NewTimer(p.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-nudge:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// Package poller turns Linear state into normalized TrackerEvents.
package poller

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	pluginv1 "github.com/soemii/orchivio-server/contract/gen/orchivio/plugin/v1"

	"github.com/soemii/orchivio-adapter-linear/internal/linear"
)

// overlap re-reads a small window so events with equal timestamps are not
// missed; the server dedups on (source, external_id).
const overlap = time.Second

// Poller owns the process-level cursors shared by all event streams.
// Cursors advance only after a whole batch was delivered (Run) or returned
// (Poll), so a failed send re-delivers on the next stream; concurrent streams
// may both deliver, the server dedups.
// ponytail: each stream polls Linear itself; share one poll loop + fan-out if many subscribers appear.
type Poller struct {
	c        *linear.Client
	teams    []string
	interval time.Duration
	log      *slog.Logger

	mu       sync.Mutex
	assigned time.Time
	comments time.Time
}

type cursors struct{ assigned, comments time.Time }

// New starts both cursors at now (no replay of old events).
func New(c *linear.Client, teams []string, now time.Time, interval time.Duration) *Poller {
	return &Poller{c: c, teams: teams, interval: interval, log: slog.Default(), assigned: now, comments: now}
}

// Poll fetches new events and commits the cursors immediately.
func (p *Poller) Poll(ctx context.Context) ([]*pluginv1.TrackerEvent, error) {
	events, next, err := p.fetch(ctx)
	if err != nil {
		return nil, err
	}
	p.commit(next)
	return events, nil
}

func (p *Poller) commit(n cursors) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n.assigned.After(p.assigned) {
		p.assigned = n.assigned
	}
	if n.comments.After(p.comments) {
		p.comments = n.comments
	}
}

// fetch returns events plus the candidate cursors without committing them.
func (p *Poller) fetch(ctx context.Context) ([]*pluginv1.TrackerEvent, cursors, error) {
	viewer, err := p.c.Viewer(ctx)
	if err != nil {
		return nil, cursors{}, err
	}
	p.mu.Lock()
	next := cursors{assigned: p.assigned, comments: p.comments}
	p.mu.Unlock()
	assignedSince, commentsSince := next.assigned.Add(-overlap), next.comments.Add(-overlap)

	assigned, assignedTruncated, err := p.c.AssignedSince(ctx, p.teams, assignedSince)
	if err != nil {
		return nil, cursors{}, err
	}
	comments, commentsTruncated, err := p.c.CommentsSince(ctx, p.teams, commentsSince)
	if err != nil {
		return nil, cursors{}, err
	}

	var events []*pluginv1.TrackerEvent
	for _, a := range assigned {
		if !assignedTruncated && a.UpdatedAt.After(next.assigned) {
			next.assigned = a.UpdatedAt
		}
		if ev := p.assignmentEvent(a, viewer, assignedSince); ev != nil {
			events = append(events, ev)
		}
	}
	for _, cm := range comments {
		if !commentsTruncated && cm.CreatedAt.After(next.comments) {
			next.comments = cm.CreatedAt
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
	if assignedTruncated || commentsTruncated {
		// ponytail: >500 events per poll re-fetches the same window; raise cap or switch to ascending order
		p.log.Warn("page cap reached, cursor not advanced", "assignments", assignedTruncated, "comments", commentsTruncated)
	}
	return events, next, nil
}

// assignmentEvent ignores history entries not newer than since: an issue
// updated for other reasons must not re-emit an old assignment.
func (p *Poller) assignmentEvent(a linear.Assignment, viewer linear.Viewer, since time.Time) *pluginv1.TrackerEvent {
	var best *linear.HistoryEntry
	for i := range a.History.Nodes {
		h := &a.History.Nodes[i]
		if toViewer(h, viewer) && h.CreatedAt.After(since) && (best == nil || h.CreatedAt.After(best.CreatedAt)) {
			best = h
		}
	}
	if best == nil && a.CreatedAt.After(since) && createdForViewer(a, viewer) {
		// Linear writes no history entry when an issue is created already
		// assigned or delegated; the creator is the assigning actor.
		best = &linear.HistoryEntry{CreatedAt: a.CreatedAt, Actor: a.Creator}
	}
	if best == nil || best.Actor == nil {
		p.log.Debug("no new assignment entry, skipping", "issue", a.Identifier)
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

// toViewer reports an assignment or (for app users) a delegation to the viewer.
func toViewer(h *linear.HistoryEntry, v linear.Viewer) bool {
	return (h.ToAssignee != nil && h.ToAssignee.ID == v.ID) || (h.ToDelegate != nil && h.ToDelegate.ID == v.ID)
}

func createdForViewer(a linear.Assignment, v linear.Viewer) bool {
	return (a.Assignee != nil && a.Assignee.ID == v.ID) || (a.Delegate != nil && a.Delegate.ID == v.ID)
}

// mentions reports an @displayName/@name token (case-insensitive, whole token)
// or a profile URL /profiles/<displayName> with the same token boundary.
func mentions(body string, v linear.Viewer) bool {
	lower := strings.ToLower(body)
	for _, name := range []string{v.DisplayName, v.Name} {
		if name != "" && hasToken(lower, "@"+strings.ToLower(name)) {
			return true
		}
	}
	return v.DisplayName != "" && hasToken(lower, "/profiles/"+strings.ToLower(v.DisplayName))
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
		if r, _ := utf8.DecodeRuneInString(s[end:]); !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return true
		}
		off += i + 1
	}
}

// Run polls, sends, then waits for the interval or a nudge. Poll errors are
// logged and retried on the next tick; it returns on ctx cancel or send error.
func (p *Poller) Run(ctx context.Context, nudge <-chan struct{}, send func(*pluginv1.TrackerEvent) error) error {
	for {
		events, next, err := p.fetch(ctx)
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
		p.commit(next)
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

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

	pluginv1 "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"

	"github.com/orchivio/adapter-linear/internal/linear"
)

// overlap re-reads a small window so events with equal timestamps are not
// missed; the server dedups on (source, external_id).
const overlap = time.Second

// Poller owns the process-level cursors per team key, shared by all event
// streams. Cursors advance only after a whole batch was delivered (Run) or
// returned (Poll), so a failed send re-delivers on the next stream; concurrent
// streams may both deliver, the server dedups.
// ponytail: each stream polls Linear itself; share one poll loop + fan-out if many subscribers appear.
type Poller struct {
	c        *linear.Client
	now      func() time.Time
	interval time.Duration
	log      *slog.Logger

	mu      sync.Mutex
	cursors map[string]cursors // by team key
}

type cursors struct{ assigned, comments time.Time }

// New creates a poller without cursors; a team key starts at now() when it
// is first requested (no replay of old events).
func New(c *linear.Client, now func() time.Time, interval time.Duration) *Poller {
	return &Poller{c: c, now: now, interval: interval, log: slog.Default(), cursors: map[string]cursors{}}
}

// Poll fetches new events for teams and commits the cursors immediately.
func (p *Poller) Poll(ctx context.Context, teams []string) ([]*pluginv1.TrackerEvent, error) {
	events, next, err := p.fetch(ctx, teams)
	if err != nil {
		return nil, err
	}
	p.commit(next)
	return events, nil
}

func (p *Poller) commit(next map[string]cursors) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for team, n := range next {
		cur := p.cursors[team]
		if n.assigned.After(cur.assigned) {
			cur.assigned = n.assigned
		}
		if n.comments.After(cur.comments) {
			cur.comments = n.comments
		}
		p.cursors[team] = cur
	}
}

// snapshot returns the cursors of teams, starting unseen teams at now().
func (p *Poller) snapshot(teams []string) map[string]cursors {
	p.mu.Lock()
	defer p.mu.Unlock()
	snap := make(map[string]cursors, len(teams))
	for _, team := range teams {
		cur, ok := p.cursors[team]
		if !ok {
			now := p.now()
			cur = cursors{assigned: now, comments: now}
			p.cursors[team] = cur
		}
		snap[team] = cur
	}
	return snap
}

// earliest returns the minimum cursor pair over all teams.
func earliest(snap map[string]cursors) cursors {
	var minimum cursors
	first := true
	for _, cur := range snap {
		if first || cur.assigned.Before(minimum.assigned) {
			minimum.assigned = cur.assigned
		}
		if first || cur.comments.Before(minimum.comments) {
			minimum.comments = cur.comments
		}
		first = false
	}
	return minimum
}

// fetch queries from the earliest cursor of teams and keeps only events after
// their own team's cursor (minus overlap). It returns the candidate cursors
// without committing them.
func (p *Poller) fetch(ctx context.Context, teams []string) ([]*pluginv1.TrackerEvent, map[string]cursors, error) {
	viewer, err := p.c.Viewer(ctx)
	if err != nil {
		return nil, nil, err
	}
	next := p.snapshot(teams)
	since := earliest(next)

	assigned, assignedTruncated, err := p.c.AssignedSince(ctx, teams, since.assigned.Add(-overlap))
	if err != nil {
		return nil, nil, err
	}
	comments, commentsTruncated, err := p.c.CommentsSince(ctx, teams, since.comments.Add(-overlap))
	if err != nil {
		return nil, nil, err
	}

	var events []*pluginv1.TrackerEvent
	for _, a := range assigned {
		cur, ok := next[a.Team.Key]
		if !ok {
			continue
		}
		if ev := p.assignmentEvent(a, viewer, cur.assigned.Add(-overlap)); ev != nil {
			events = append(events, ev)
		}
		if !assignedTruncated && a.UpdatedAt.After(cur.assigned) {
			cur.assigned = a.UpdatedAt
			next[a.Team.Key] = cur
		}
	}
	for _, cm := range comments {
		cur, ok := next[cm.Issue.Team.Key]
		if !ok || !cm.CreatedAt.After(cur.comments.Add(-overlap)) {
			continue
		}
		if !commentsTruncated && cm.CreatedAt.After(cur.comments) {
			cur.comments = cm.CreatedAt
			next[cm.Issue.Team.Key] = cur
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

// Run polls teams, sends, then waits for the interval or a nudge. Poll errors are
// logged and retried on the next tick; it returns on ctx cancel or send error.
func (p *Poller) Run(ctx context.Context, teams []string, nudge <-chan struct{}, send func(*pluginv1.TrackerEvent) error) error {
	for {
		events, next, err := p.fetch(ctx, teams)
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

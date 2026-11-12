package poller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/soemii/orchivio-server/contract/gen/orchivio/plugin/v1"

	"github.com/soemii/orchivio-adapter-linear/internal/linear"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

const viewerJSON = `{"viewer":{"id":"bot","name":"Orchivio Bot","displayName":"orchivio","email":"bot@x"}}`

// fake routes by query content; comments/assigned are mutable per test.
type fake struct {
	mu       sync.Mutex
	comments string // JSON array
	issues   string // JSON array
	sinces   []string
	hasNext  bool
}

func (f *fake) client(t *testing.T) *linear.Client {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case strings.Contains(req.Query, "viewer"):
			w.Write([]byte(`{"data":` + viewerJSON + `}`))
		case strings.Contains(req.Query, "comments("):
			since, _ := time.Parse(time.RFC3339Nano, req.Variables["since"].(string))
			f.sinces = append(f.sinces, req.Variables["since"].(string))
			var all, kept []json.RawMessage
			var served string
			json.Unmarshal([]byte(f.comments), &all)
			for _, raw := range all {
				var c struct{ CreatedAt time.Time }
				json.Unmarshal(raw, &c)
				if c.CreatedAt.After(since) {
					kept = append(kept, raw)
				}
			}
			filtered, _ := json.Marshal(kept)
			served = string(filtered)
			w.Write([]byte(`{"data":{"comments":{"nodes":` + served + `,"pageInfo":{"hasNextPage":` + strconv.FormatBool(f.hasNext) + `,"endCursor":"e"}}}}`))
		case strings.Contains(req.Query, "issues("):
			w.Write([]byte(`{"data":{"issues":{"nodes":` + f.issues + `,"pageInfo":{"hasNextPage":false}}}}`))
		default:
			http.Error(w, "unexpected", 500)
		}
	}))
	t.Cleanup(srv.Close)
	return linear.New(srv.URL, "k")
}

func comment(id, body, userID string, at time.Time) string {
	user := `null`
	if userID != "" {
		user = `{"id":"` + userID + `","email":"` + userID + `@x","name":"N"}`
	}
	return `{"id":"` + id + `","body":` + quote(body) + `,"createdAt":"` + at.Format(time.RFC3339Nano) + `","user":` + user + `,"issue":{"identifier":"ORC-1","team":{"key":"ORC"}}}`
}

func quote(s string) string { b, _ := json.Marshal(s); return string(b) }

func TestPollSkipsOwnComments(t *testing.T) {
	f := &fake{issues: `[]`, comments: `[` + comment("c1", "done", "bot", t0.Add(time.Minute)) + `]`}
	evs, err := New(f.client(t), []string{"ORC"}, t0, time.Hour).Poll(context.Background())
	if err != nil || len(evs) != 0 {
		t.Fatalf("evs=%v err=%v", evs, err)
	}
}

func TestPollDropsBotCommentsWithoutUser(t *testing.T) {
	f := &fake{issues: `[]`, comments: `[` + comment("c1", "hi", "", t0.Add(time.Minute)) + `]`}
	evs, _ := New(f.client(t), []string{"ORC"}, t0, time.Hour).Poll(context.Background())
	if len(evs) != 0 {
		t.Fatalf("evs=%v", evs)
	}
}

func TestPollDetectsMention(t *testing.T) {
	f := &fake{issues: `[]`, comments: `[` + comment("c1", "@orchivio was macht X?", "u1", t0.Add(time.Minute)) + `,` +
		comment("c2", "plain", "u1", t0.Add(time.Minute)) + `]`}
	evs, err := New(f.client(t), []string{"ORC"}, t0, time.Hour).Poll(context.Background())
	if err != nil || len(evs) != 2 {
		t.Fatalf("evs=%v err=%v", evs, err)
	}
	if !evs[0].MentionsBot || evs[1].MentionsBot {
		t.Fatalf("mentions: %v %v", evs[0].MentionsBot, evs[1].MentionsBot)
	}
	if evs[0].ExternalId != "comment:c1" || evs[0].Kind != pluginv1.TrackerEvent_COMMENTED || evs[0].ProjectKey != "ORC" || evs[0].Actor.Email != "u1@x" {
		t.Fatalf("ev=%v", evs[0])
	}
}

func TestMentionsBoundaries(t *testing.T) {
	v := linear.Viewer{Name: "Orchivio Bot", DisplayName: "orchivio"}
	cases := map[string]bool{
		"@orchivio":                 true,
		"hey @Orchivio, look":       true,
		"@orchivio.":                true,
		"@orchiviox no":             false,
		"@orchivio_2":               false,
		"mail me":                   false,
		"@orchiviox then @orchivio": true,
		"@Orchivio Bot please":      true,
		"see https://linear.app/x/profiles/orchivio": true,
		"https://linear.app/x/profiles/orchiviox":    false,
	}
	for body, want := range cases {
		if got := mentions(body, v); got != want {
			t.Errorf("mentions(%q) = %v, want %v", body, got, want)
		}
	}
}

func TestPollAssignmentUsesAssigningActor(t *testing.T) {
	f := &fake{comments: `[]`, issues: `[{"id":"i1","identifier":"ORC-1","updatedAt":"` + t0.Add(time.Minute).Format(time.RFC3339) + `","team":{"key":"ORC"},"history":{"nodes":[
	 {"createdAt":"` + t0.Add(2*time.Second).Format(time.RFC3339) + `","actor":{"id":"x","email":"other@x","name":"O"},"toAssignee":null},
	 {"createdAt":"` + t0.Add(time.Second).Format(time.RFC3339) + `","actor":{"id":"pm","email":"pm@x","name":"PM"},"toAssignee":{"id":"bot"}}]}}]`}
	evs, err := New(f.client(t), []string{"ORC"}, t0, time.Hour).Poll(context.Background())
	if err != nil || len(evs) != 1 {
		t.Fatalf("evs=%v err=%v", evs, err)
	}
	ev := evs[0]
	if ev.Kind != pluginv1.TrackerEvent_ASSIGNED || ev.Actor.Email != "pm@x" || ev.ProjectKey != "ORC" || ev.TicketRef != "ORC-1" {
		t.Fatalf("ev=%v", ev)
	}
	if ev.ExternalId != "assign:i1:"+t0.Add(time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("external id = %q", ev.ExternalId)
	}
}

func TestPollSkipsAssignmentWithoutHistory(t *testing.T) {
	f := &fake{comments: `[]`, issues: `[{"id":"i1","identifier":"ORC-1","updatedAt":"` + t0.Format(time.RFC3339) + `","team":{"key":"ORC"},"history":{"nodes":[]}}]`}
	evs, _ := New(f.client(t), []string{"ORC"}, t0, time.Hour).Poll(context.Background())
	if len(evs) != 0 {
		t.Fatalf("evs=%v", evs)
	}
}

func TestPollAdvancesCursor(t *testing.T) {
	last := t0.Add(time.Minute)
	f := &fake{issues: `[]`, comments: `[` + comment("c1", "hi", "u1", last) + `]`}
	p := New(f.client(t), []string{"ORC"}, t0, time.Hour)
	p.Poll(context.Background())
	p.Poll(context.Background())
	if f.sinces[0] != t0.Add(-overlap).Format(time.RFC3339Nano) {
		t.Fatalf("first since = %s", f.sinces[0])
	}
	if want := last.Add(-overlap).Format(time.RFC3339Nano); f.sinces[1] != want {
		t.Fatalf("second since = %s, want %s", f.sinces[1], want)
	}
}

func TestRunNudgeTriggersImmediatePoll(t *testing.T) {
	f := &fake{issues: `[]`, comments: `[` + comment("c1", "hi", "u1", t0.Add(time.Minute)) + `]`}
	p := New(f.client(t), []string{"ORC"}, t0, time.Hour)
	nudge := make(chan struct{}, 1)
	got := make(chan *pluginv1.TrackerEvent)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx, nudge, func(ev *pluginv1.TrackerEvent) error { got <- ev; return nil })
	<-got // first poll
	nudge <- struct{}{}
	<-got // second poll only possible via nudge (interval = 1h)
}

func TestRunSendFailureRedeliversOnNextStream(t *testing.T) {
	f := &fake{issues: `[]`, comments: `[` + comment("c1", "a", "u1", t0.Add(time.Minute)) + `,` + comment("c2", "b", "u1", t0.Add(2*time.Minute)) + `]`}
	p := New(f.client(t), []string{"ORC"}, t0, time.Hour)
	sent := 0
	err := p.Run(context.Background(), nil, func(*pluginv1.TrackerEvent) error {
		sent++
		if sent == 2 {
			return context.Canceled
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected send error")
	}
	var ids []string
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	p.Run(ctx, nil, func(ev *pluginv1.TrackerEvent) error {
		ids = append(ids, ev.ExternalId)
		if len(ids) == 2 {
			cancel()
		}
		return nil
	})
	if strings.Join(ids, ",") != "comment:c1,comment:c2" {
		t.Fatalf("ids=%v", ids)
	}
}

func TestPollDoesNotAdvanceCursorWhenPageCapHit(t *testing.T) {
	f := &fake{issues: `[]`, hasNext: true, comments: `[` + comment("c1", "a", "u1", t0.Add(time.Minute)) + `]`}
	p := New(f.client(t), []string{"ORC"}, t0, time.Hour)
	p.Poll(context.Background())
	f.sinces = nil
	p.Poll(context.Background())
	if want := t0.Add(-overlap).Format(time.RFC3339Nano); f.sinces[0] != want {
		t.Fatalf("since = %s, want unchanged %s", f.sinces[0], want)
	}
}

func TestPollIgnoresStaleAssignmentHistory(t *testing.T) {
	f := &fake{comments: `[]`, issues: `[{"id":"i1","identifier":"ORC-1","updatedAt":"` + t0.Add(time.Minute).Format(time.RFC3339) + `","team":{"key":"ORC"},"history":{"nodes":[
	 {"createdAt":"` + t0.Add(-time.Hour).Format(time.RFC3339) + `","actor":{"id":"pm","email":"pm@x","name":"PM"},"toAssignee":{"id":"bot"}}]}}]`}
	evs, _ := New(f.client(t), []string{"ORC"}, t0, time.Hour).Poll(context.Background())
	if len(evs) != 0 {
		t.Fatalf("evs=%v", evs)
	}
}

func TestPollDelegationToAppUserIsAssignment(t *testing.T) {
	f := &fake{comments: `[]`, issues: `[{"id":"i1","identifier":"ORC-1","updatedAt":"` + t0.Add(time.Minute).Format(time.RFC3339) + `","team":{"key":"ORC"},"history":{"nodes":[
	 {"createdAt":"` + t0.Add(time.Second).Format(time.RFC3339) + `","actor":{"id":"pm","email":"pm@x","name":"PM"},"toAssignee":null,"toDelegate":{"id":"bot"}}]}}]`}
	evs, err := New(f.client(t), []string{"ORC"}, t0, time.Hour).Poll(context.Background())
	if err != nil || len(evs) != 1 || evs[0].Kind != pluginv1.TrackerEvent_ASSIGNED || evs[0].Actor.Email != "pm@x" {
		t.Fatalf("evs=%v err=%v", evs, err)
	}
}

func TestPollIssueCreatedDelegatedToAppUserIsAssignment(t *testing.T) {
	created := t0.Add(time.Second)
	f := &fake{comments: `[]`, issues: `[{"id":"i1","identifier":"RES-9","createdAt":"` + created.Format(time.RFC3339) + `","updatedAt":"` + created.Format(time.RFC3339) + `","team":{"key":"RES"},
	 "creator":{"id":"pm","email":"pm@x","name":"PM"},"assignee":{"id":"pm"},"delegate":{"id":"bot"},"history":{"nodes":[]}}]`}
	evs, err := New(f.client(t), []string{"RES"}, t0, time.Hour).Poll(context.Background())
	if err != nil || len(evs) != 1 || evs[0].Kind != pluginv1.TrackerEvent_ASSIGNED || evs[0].Actor.Email != "pm@x" {
		t.Fatalf("evs=%v err=%v", evs, err)
	}
	if evs[0].ExternalId != "assign:i1:"+created.Format(time.RFC3339Nano) {
		t.Fatalf("external id = %q", evs[0].ExternalId)
	}
}

func TestPollIgnoresOldIssueDelegatedWithoutHistory(t *testing.T) {
	f := &fake{comments: `[]`, issues: `[{"id":"i1","identifier":"RES-9","createdAt":"` + t0.Add(-time.Hour).Format(time.RFC3339) + `","updatedAt":"` + t0.Add(time.Minute).Format(time.RFC3339) + `","team":{"key":"RES"},
	 "creator":{"id":"pm","email":"pm@x","name":"PM"},"delegate":{"id":"bot"},"history":{"nodes":[]}}]`}
	evs, _ := New(f.client(t), []string{"RES"}, t0, time.Hour).Poll(context.Background())
	if len(evs) != 0 {
		t.Fatalf("evs=%v", evs)
	}
}

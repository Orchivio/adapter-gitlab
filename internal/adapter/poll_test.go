package adapter

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// fakeStream is the server side of StreamEvents without a network.
type fakeStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent []*pb.CodeHostEvent
}

func (f *fakeStream) Context() context.Context { return f.ctx }
func (f *fakeStream) Send(e *pb.CodeHostEvent) error {
	f.sent = append(f.sent, e)
	return nil
}

// streamCycles runs StreamEvents for the given number of poll cycles and
// returns what it sent.
func streamCycles(t *testing.T, s *Server, repos []string, cycles int) []*pb.CodeHostEvent {
	t.Helper()
	return streamCyclesWith(t, s, []*pb.Credential{groupCredential(t, "g", fakeToken)}, repos, cycles)
}

func streamCyclesWith(t *testing.T, s *Server, creds []*pb.Credential, repos []string, cycles int) []*pb.CodeHostEvent {
	t.Helper()
	polled, ticks := make(chan struct{}), make(chan time.Time)
	s.after = func(d time.Duration) <-chan time.Time {
		if d != 30*time.Second {
			t.Errorf("interval = %v", d)
		}
		polled <- struct{}{}
		return ticks
	}
	ctx, cancel := context.WithCancel(t.Context())
	stream := &fakeStream{ctx: ctx}
	done := make(chan error, 1)
	go func() {
		done <- s.StreamEvents(&pb.StreamCodeHostEventsRequest{Repos: repos, Credentials: creds}, stream)
	}()
	for i := range cycles {
		<-polled
		if i < cycles-1 {
			ticks <- fixedNow
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("StreamEvents = %v", err)
	}
	return stream.sent
}

func externalIDs(events []*pb.CodeHostEvent) []string {
	var out []string
	for _, e := range events {
		out = append(out, e.GetExternalId())
	}
	return out
}

const ourMR = `{"iid":7,"source_branch":"orchivio/abc-1-fix","source_project_id":42,"target_project_id":42,"web_url":"https://gitlab.example.test/g/p/-/merge_requests/7"}`

var recent = fixedNow.Add(time.Second).Format(time.RFC3339)

func humanNote(id int, body, updated string) string {
	return fmt.Sprintf(`{"id":%d,"body":%q,"system":false,"updated_at":%q,"author":{"id":6,"username":"dev","name":"Dev One"}}`, id, body, updated)
}

// pollFake serves one repo g/p with the given merge requests and notes keyed
// "<iid>" (page 1) or "<iid>/<page>", and records each request URI; other
// repos answer 500.
type pollFake struct {
	*httptest.Server
	mu   sync.Mutex
	uris []string
}

func newPollFake(t *testing.T, mrs string, notes map[string]string) *pollFake {
	t.Helper()
	f := &pollFake{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.uris = append(f.uris, r.URL.RequestURI())
		f.mu.Unlock()
		path := r.URL.EscapedPath()
		prefix := "/api/v4/projects/g%2Fp/merge_requests"
		switch {
		case !strings.HasPrefix(path, prefix):
			w.WriteHeader(http.StatusInternalServerError)
		case path == prefix:
			fmt.Fprint(w, mrs)
		case strings.HasSuffix(path, "/notes"):
			key := strings.Split(strings.TrimPrefix(path, prefix+"/"), "/")[0]
			if page := r.URL.Query().Get("page"); page != "" && page != "1" {
				key += "/" + page
			}
			fmt.Fprint(w, "["+notes[key]+"]")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *pollFake) requests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.uris
}

func TestPollSkipsOwnAndSystemNotes(t *testing.T) {
	notes := strings.Join([]string{
		`{"id":100,"body":"changed the description","system":true,"updated_at":"` + recent + `","author":{"id":6,"username":"dev"}}`,
		`{"id":101,"body":"Run started","system":false,"updated_at":"` + recent + `","author":{"id":5,"username":"orchivio-bot"}}`,
		`{"id":102,"body":"no author","system":false,"updated_at":"` + recent + `"}`,
		humanNote(103, "older than the cursor", fixedNow.Add(-time.Hour).Format(time.RFC3339)),
		humanNote(104, "looks good", recent),
	}, ",")
	f := newPollFake(t, "["+ourMR+"]", map[string]string{"7": notes})
	s := newServer(t, f.URL)

	events := streamCycles(t, s, []string{"g/p"}, 1)

	if got := externalIDs(events); !reflect.DeepEqual(got, []string{"note:104"}) {
		t.Fatalf("events = %v", got)
	}
}

func TestPollIgnoresForksAndForeignBranches(t *testing.T) {
	mrs := `[{"iid":8,"source_branch":"feature/x","source_project_id":42,"target_project_id":42,"web_url":"u8"},
	{"iid":9,"source_branch":"orchivio/abc-1","source_project_id":99,"target_project_id":42,"web_url":"u9"}]`
	f := newPollFake(t, mrs, map[string]string{"8": humanNote(200, "hi", recent), "9": humanNote(300, "hi", recent)})
	s := newServer(t, f.URL)

	events := streamCycles(t, s, []string{"g/p"}, 1)

	if len(events) != 0 {
		t.Fatalf("events = %v", externalIDs(events))
	}
}

func TestPollEmitsMentionOnMR(t *testing.T) {
	f := newPollFake(t, "["+ourMR+"]", map[string]string{"7": humanNote(104, "@orchivio-bot bitte X", recent)})
	s := newServer(t, f.URL)

	events := streamCycles(t, s, []string{"g/p"}, 1)

	want := &pb.CodeHostEvent{ExternalId: "note:104", Kind: pb.CodeHostEvent_REVIEW_COMMENTED, Repo: "g/p", Branch: "orchivio/abc-1-fix",
		PrUrl: "https://gitlab.example.test/g/p/-/merge_requests/7", Actor: &pb.Actor{Id: "6", Username: "dev", Name: "Dev One"},
		Body: "@orchivio-bot bitte X", MentionsBot: true}
	if len(events) != 1 || !proto.Equal(events[0], want) {
		t.Fatalf("events = %v", events)
	}
	wantRequests := []string{
		"/api/v4/projects/g%2Fp/merge_requests?order_by=updated_at&per_page=100&state=opened&updated_after=2026-11-26T10%3A00%3A00Z",
		"/api/v4/projects/g%2Fp/merge_requests/7/notes?order_by=updated_at&page=1&per_page=100&sort=desc",
	}
	if got := f.requests(); !reflect.DeepEqual(got, wantRequests) {
		t.Fatalf("requests = %v", got)
	}
}

func TestPollMentionNeedsExactUsername(t *testing.T) {
	notes := humanNote(104, "mail dev@orchivio-bot.example", recent) + "," + humanNote(105, "@orchivio-bot-alt hi", recent)
	f := newPollFake(t, "["+ourMR+"]", map[string]string{"7": notes})
	s := newServer(t, f.URL)

	events := streamCycles(t, s, []string{"g/p"}, 1)

	if len(events) != 2 || events[0].GetMentionsBot() || events[1].GetMentionsBot() {
		t.Fatalf("events = %v", events)
	}
}

func TestPollDedupsAcrossOverlap(t *testing.T) {
	f := newPollFake(t, "["+ourMR+"]", map[string]string{"7": humanNote(104, "once", recent)})
	s := newServer(t, f.URL)

	events := streamCycles(t, s, []string{"g/p"}, 2)

	if got := externalIDs(events); !reflect.DeepEqual(got, []string{"note:104"}) {
		t.Fatalf("events = %v", got)
	}
}

func TestPollContinuesAfterFailingRepo(t *testing.T) {
	f := newPollFake(t, "["+ourMR+"]", map[string]string{"7": humanNote(104, "hi", recent)})
	s := newServer(t, f.URL)

	events := streamCycles(t, s, []string{"g/broken", "other/uncovered", "g/p"}, 1)

	if got := externalIDs(events); !reflect.DeepEqual(got, []string{"note:104"}) {
		t.Fatalf("events = %v", got)
	}
}

func notesAt(first, count int, system bool, updated string) string {
	var out []string
	for i := range count {
		out = append(out, fmt.Sprintf(`{"id":%d,"body":"n","system":%v,"updated_at":%q,"author":{"id":6,"username":"dev"}}`, first+i, system, updated))
	}
	return strings.Join(out, ",")
}

func TestPollPagesNotesNewestFirst(t *testing.T) {
	newer := fixedNow.Add(2 * time.Second).Format(time.RFC3339)
	old := fixedNow.Add(-time.Hour).Format(time.RFC3339)
	f := newPollFake(t, "["+ourMR+"]", map[string]string{
		"7":   humanNote(1, "newest", newer) + "," + notesAt(1000, 99, true, recent),
		"7/2": humanNote(2, "older", recent) + "," + notesAt(2000, 99, false, old),
		"7/3": humanNote(3, "beyond the cursor page", recent),
	})
	s := newServer(t, f.URL)

	events := streamCycles(t, s, []string{"g/p"}, 1)

	if got := externalIDs(events); !reflect.DeepEqual(got, []string{"note:2", "note:1"}) {
		t.Fatalf("events = %v", got)
	}
	if got := len(f.requests()); got != 3 {
		t.Fatalf("requests = %v; want MR list and two note pages", f.requests())
	}
}

func unauthorizedFake(t *testing.T) *httptest.Server {
	t.Helper()
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(fake.Close)
	return fake
}

func TestStreamEndsUnauthenticatedWithSingleConnection(t *testing.T) {
	s := newServer(t, unauthorizedFake(t).URL)

	err := s.StreamEvents(&pb.StreamCodeHostEventsRequest{Repos: []string{"g/p"},
		Credentials: []*pb.Credential{groupCredential(t, "g", fakeToken)}}, &fakeStream{ctx: t.Context()})

	if status.Code(err) != codes.Unauthenticated || strings.Contains(err.Error(), fakeToken) {
		t.Fatalf("StreamEvents = %v; want Unauthenticated", err)
	}
}

func TestStreamKeepsRunningOn401WithSeveralConnections(t *testing.T) {
	s := newServer(t, unauthorizedFake(t).URL)
	creds := []*pb.Credential{groupCredential(t, "g", fakeToken), groupCredential(t, "h", "tok-h")}

	events := streamCyclesWith(t, s, creds, []string{"g/p", "h/q"}, 2)

	if len(events) != 0 {
		t.Fatalf("events = %v", events)
	}
}

func TestStreamWithoutCoveredRepoIsFailedPrecondition(t *testing.T) {
	s := newServer(t, "https://gitlab.example.test")

	err := s.StreamEvents(&pb.StreamCodeHostEventsRequest{Repos: []string{"other/p"},
		Credentials: []*pb.Credential{groupCredential(t, "g", fakeToken)}}, &fakeStream{ctx: t.Context()})

	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StreamEvents = %v; want FailedPrecondition", err)
	}
}

func TestNotePageCapLogsRepoAndMR(t *testing.T) {
	page := make([]string, pageSize)
	for i := range page {
		page[i] = humanNote(i+1, "secret body", recent)
	}
	var pages atomic.Int32
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pages.Add(1)
		fmt.Fprint(w, "["+strings.Join(page, ",")+"]")
	}))
	t.Cleanup(fake.Close)
	s := newServer(t, fake.URL)
	c, err := s.connectionFor([]*pb.Credential{groupCredential(t, "g", fakeToken)}, "g/p")
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	notes, err := c.notesSince(t.Context(), "g/p", 7, fixedNow)

	if err != nil || len(notes) != maxNotePage*pageSize || pages.Load() != maxNotePage {
		t.Fatalf("err=%v notes=%d pages=%d", err, len(notes), pages.Load())
	}
	got := logs.String()
	if !strings.Contains(got, "level=WARN") || !strings.Contains(got, "repo=g/p") || !strings.Contains(got, "merge_request=7") ||
		strings.Contains(got, "secret") || strings.Contains(got, fakeToken) {
		t.Fatalf("log = %q", got)
	}
}

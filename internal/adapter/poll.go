package adapter

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	pb "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"github.com/orchivio/orchestrator/contract/redact"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// overlap covers clock skew between GitLab and the adapter and late writes.
	overlap      = 2 * time.Minute
	branchPrefix = "orchivio/"
	// ponytail: one page of 100 MRs per repo per tick; follow X-Next-Page if
	// a repo ever has more updated MRs within one window.
	pageSize    = 100
	maxNotePage = 50 // stops a misbehaving server from paging forever
)

// cursorKey keeps read state per connection bot and repo.
type cursorKey struct {
	bot  int64
	repo string
}

type pollCursor struct {
	started, watermark time.Time
	seen               map[string]time.Time
}

type mergeRequest struct {
	IID             int64  `json:"iid"`
	SourceBranch    string `json:"source_branch"`
	SourceProjectID int64  `json:"source_project_id"`
	TargetProjectID int64  `json:"target_project_id"`
	WebURL          string `json:"web_url"`
}

type note struct {
	ID        int64
	Body      string
	System    bool
	UpdatedAt time.Time `json:"updated_at"`
	Author    struct {
		ID             int64
		Username, Name string
	}
}

// target is one stream repo and the connection that covers it. authFatal
// ends the stream on HTTP 401 so the orchestrator marks the connection
// needs_reauth; with several connections it cannot tell which one, so the
// repo is only logged and skipped.
type target struct {
	repo      string
	conn      conn
	authFatal bool
}

// StreamEvents polls the requested repos' merge request notes until the
// client or the server goes away. A repo without a covering connection or
// with a failing fetch is logged and skipped; the others keep flowing. No
// covered repo at all is FAILED_PRECONDITION.
func (s *Server) StreamEvents(req *pb.StreamCodeHostEventsRequest, stream pb.CodeHostAdapter_StreamEventsServer) error {
	if len(req.GetRepos()) == 0 {
		return status.Error(codes.InvalidArgument, "repos must not be empty")
	}
	conns, err := s.connections(req.GetCredentials())
	if err != nil {
		return err
	}
	bots := make(map[int64]bool, len(conns))
	for _, c := range conns {
		bots[c.cred.BotID] = true
	}
	var targets []target
	for _, repo := range req.GetRepos() {
		c, err := pick(conns, repo)
		if err != nil {
			slog.Warn("GitLab repo not polled", "repo", repo, "error", err)
			continue
		}
		targets = append(targets, target{repo, c, len(conns) == 1})
	}
	if len(targets) == 0 {
		return status.Error(codes.FailedPrecondition, "no GitLab connection covers the requested repos")
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	for {
		for _, t := range targets {
			if err := s.poll(ctx, t, bots, s.now(), stream.Send); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-s.after(s.interval):
		}
	}
}

// poll sends the repo's unseen notes since its watermark minus overlap. A
// failed fetch is logged and retried on the next tick; a failed send, or a
// 401 with a single connection, ends the stream.
func (s *Server) poll(ctx context.Context, t target, bots map[int64]bool, now time.Time, send func(*pb.CodeHostEvent) error) error {
	s.mu.Lock()
	key := cursorKey{t.conn.cred.BotID, t.repo}
	cur := s.cursors[key]
	if cur == nil {
		cur = &pollCursor{started: now, watermark: now, seen: make(map[string]time.Time)}
		s.cursors[key] = cur
	}
	window := cur.watermark.Add(-overlap)
	if cur.started.After(window) {
		window = cur.started
	}
	s.mu.Unlock()

	events, err := s.fetch(ctx, t, bots, window)
	if t.authFatal && errors.Is(err, apiError(http.StatusUnauthorized)) {
		return grpcError(err)
	}
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("GitLab polling failed", "repo", t.repo, "error", err)
		}
		return nil
	}
	for _, item := range events {
		id := item.event.GetExternalId()
		s.mu.Lock()
		_, seen := cur.seen[id]
		s.mu.Unlock()
		if seen {
			continue
		}
		if err := send(item.event); err != nil {
			return err
		}
		s.mu.Lock()
		cur.seen[id] = item.at
		s.mu.Unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if now.After(cur.watermark) {
		cur.watermark = now
	}
	for id, at := range cur.seen {
		if at.Before(cur.watermark.Add(-overlap)) {
			delete(cur.seen, id)
		}
	}
	return nil
}

type datedEvent struct {
	event *pb.CodeHostEvent
	at    time.Time
}

func (s *Server) fetch(ctx context.Context, t target, bots map[int64]bool, since time.Time) ([]datedEvent, error) {
	var mrs []mergeRequest
	query := url.Values{"updated_after": {since.UTC().Format(time.RFC3339)}, "state": {"opened"},
		"order_by": {"updated_at"}, "per_page": {strconv.Itoa(pageSize)}}
	if err := t.conn.request(ctx, http.MethodGet, projectPath(t.repo)+"/merge_requests", query, nil, &mrs); err != nil {
		return nil, err
	}
	mention := mentionPattern(t.conn.cred.BotUsername)
	var events []datedEvent
	for _, mr := range mrs {
		// Own branches only, and no forks: a fork's branch is not ours to push.
		if !strings.HasPrefix(mr.SourceBranch, branchPrefix) || mr.SourceProjectID != mr.TargetProjectID || mr.IID <= 0 {
			continue
		}
		notes, err := t.conn.notesSince(ctx, t.repo, mr.IID, since)
		if errors.Is(err, apiError(http.StatusNotFound)) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, n := range notes {
			if n.System || bots[n.Author.ID] || n.UpdatedAt.Before(since) {
				continue
			}
			if n.ID <= 0 || n.Author.ID <= 0 || n.Author.Username == "" {
				slog.Warn("GitLab note skipped: incomplete", "repo", t.repo, "merge_request", mr.IID, "note", n.ID)
				continue
			}
			events = append(events, datedEvent{&pb.CodeHostEvent{
				ExternalId: "note:" + strconv.FormatInt(n.ID, 10), Kind: pb.CodeHostEvent_REVIEW_COMMENTED,
				Repo: t.repo, Branch: mr.SourceBranch, PrUrl: mr.WebURL,
				Actor: &pb.Actor{Id: strconv.FormatInt(n.Author.ID, 10), Username: n.Author.Username, Name: n.Author.Name},
				Body:  redact.String(n.Body), MentionsBot: mention.MatchString(n.Body),
			}, n.UpdatedAt})
		}
	}
	slices.SortStableFunc(events, func(a, b datedEvent) int { return a.at.Compare(b.at) })
	return events, nil
}

// notesSince reads an MR's notes newest first until a page reaches notes
// older than since; the caller sorts ascending.
func (c conn) notesSince(ctx context.Context, repo string, iid int64, since time.Time) ([]note, error) {
	path := projectPath(repo) + "/merge_requests/" + strconv.FormatInt(iid, 10) + "/notes"
	var all []note
	for page := 1; page <= maxNotePage; page++ {
		var notes []note
		query := url.Values{"sort": {"desc"}, "order_by": {"updated_at"}, "per_page": {strconv.Itoa(pageSize)}, "page": {strconv.Itoa(page)}}
		if err := c.request(ctx, http.MethodGet, path, query, nil, &notes); err != nil {
			return nil, err
		}
		all = append(all, notes...)
		if len(notes) < pageSize || notes[len(notes)-1].UpdatedAt.Before(since) {
			return all, nil
		}
	}
	return all, nil
}

// mentionPattern matches @username as a whole GitLab mention: not inside an
// e-mail address and not as the prefix of a longer username.
func mentionPattern(username string) *regexp.Regexp {
	return regexp.MustCompile(`(?i)(^|[^a-z0-9_.@-])@` + regexp.QuoteMeta(username) + `(?:$|[^a-z0-9_.-]|[.-](?:$|[^a-z0-9_.-]))`)
}

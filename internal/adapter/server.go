// Package adapter implements the plugin.v1 CodeHostAdapter for one GitLab
// instance. It keeps no tenant access: every call carries the tenant's
// connections (one per group or project).
package adapter

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	pb "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Config is the operator level (Ebene 1): which GitLab instance this process serves.
type Config struct {
	URL          string
	PollInterval time.Duration
}

type Server struct {
	pb.UnimplementedCodeHostAdapterServer
	http     *http.Client
	base     *url.URL
	interval time.Duration
	now      func() time.Time
	after    func(time.Duration) <-chan time.Time
	ctx      context.Context
	cancel   context.CancelFunc

	mu      sync.Mutex
	cursors map[cursorKey]*pollCursor // read state per connection bot and repo
}

var pathSegment = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]*$`)

// ValidPath reports whether path is a GitLab namespace or project path.
func ValidPath(path string) bool {
	for _, segment := range strings.Split(path, "/") {
		if !pathSegment.MatchString(segment) {
			return false
		}
	}
	return true
}

func New(cfg Config) (*Server, error) {
	base, err := parseBase(cfg.URL)
	if err != nil {
		return nil, errors.New("GITLAB_URL: " + err.Error())
	}
	if cfg.PollInterval <= 0 {
		return nil, errors.New("GITLAB_POLL_INTERVAL must be positive")
	}
	client, err := newHTTPClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{http: client, base: base, interval: cfg.PollInterval, now: time.Now, after: time.After,
		ctx: ctx, cancel: cancel, cursors: make(map[cursorKey]*pollCursor)}, nil
}

// Close ends open streams so GracefulStop does not wait for them.
func (s *Server) Close() { s.cancel() }

func (s *Server) GetCapabilities(context.Context, *pb.GetCapabilitiesRequest) (*pb.Capabilities, error) {
	return &pb.Capabilities{AdapterName: "gitlab", ContractVersion: "plugin.v1", SupportsReviewEvents: true, ConnectFields: connectFields}, nil
}

func projectPath(repo string) string { return "/projects/" + url.PathEscape(repo) }

func (s *Server) OpenPullRequest(ctx context.Context, req *pb.OpenPullRequestRequest) (*pb.PullRequest, error) {
	if req.GetBase() == "" || req.GetHead() == "" || strings.TrimSpace(req.GetTitle()) == "" {
		return nil, status.Error(codes.InvalidArgument, "base, head and title are required")
	}
	c, err := s.connectionFor(req.GetCredentials(), req.GetRepo())
	if err != nil {
		return nil, err
	}
	var result struct {
		URL string `json:"web_url"`
		IID int64
	}
	body := map[string]string{
		"source_branch": req.GetHead(), "target_branch": req.GetBase(), "title": req.GetTitle(), "description": req.GetBody(),
	}
	if err := c.request(ctx, http.MethodPost, projectPath(req.GetRepo())+"/merge_requests", nil, body, &result); err != nil {
		return nil, grpcError(err)
	}
	if result.IID <= 0 || result.URL == "" {
		return nil, status.Error(codes.Unavailable, "GitLab returned an incomplete merge request")
	}
	return &pb.PullRequest{Url: result.URL, Number: result.IID}, nil
}

// CheckAccess denies repos no connection covers or the connection cannot see.
func (s *Server) CheckAccess(ctx context.Context, req *pb.CheckAccessRequest) (*pb.CheckAccessResponse, error) {
	conns, err := s.connections(req.GetCredentials())
	if err != nil {
		return nil, err
	}
	resp := &pb.CheckAccessResponse{}
	for _, repo := range req.GetItems() {
		c, err := pick(conns, repo)
		if err == nil {
			err = c.request(ctx, http.MethodGet, projectPath(repo), nil, nil, nil)
		}
		switch {
		case err == nil:
		case status.Code(err) == codes.FailedPrecondition || status.Code(err) == codes.InvalidArgument,
			errors.Is(err, apiError(http.StatusNotFound)), errors.Is(err, apiError(http.StatusForbidden)):
			resp.Denied = append(resp.Denied, repo)
		default:
			return nil, grpcError(err)
		}
	}
	return resp, nil
}

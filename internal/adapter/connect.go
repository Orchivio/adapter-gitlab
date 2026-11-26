package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	pb "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maintainerAccess is GitLab's Maintainer access level; creating project
// access tokens needs it.
const maintainerAccess = 40

// credential is what the orchestrator stores per tenant connection: one
// GitLab group or project and the token of its technical user.
type credential struct {
	Path        string `json:"path"`
	ID          int64  `json:"id"`
	Kind        string `json:"kind"` // "group" or "project"
	Token       string `json:"token"`
	BotID       int64  `json:"botId"`
	BotUsername string `json:"botUsername"`
}

var connectFields = []*pb.ConnectField{
	{Name: "path", Label: "Group or project path", Required: true},
	{Name: "token", Label: "Access token (scope api, Maintainer role)", Secret: true, Required: true},
}

// conn is one tenant connection, rebuilt from the credential on every call.
type conn struct {
	http *http.Client
	base *url.URL
	cred credential
}

func (c conn) request(ctx context.Context, method, path string, query url.Values, in, out any) error {
	return request(ctx, c.http, c.base, "/api/v4"+path, method, query, func(r *http.Request) {
		r.Header.Set("PRIVATE-TOKEN", c.cred.Token)
	}, in, out)
}

// covers reports whether repo is the connection's project or lies below its group.
func (c conn) covers(repo string) bool {
	return repo == c.cred.Path || strings.HasPrefix(repo, c.cred.Path+"/")
}

// connections decodes stored credentials; errors never echo their data.
func (s *Server) connections(creds []*pb.Credential) ([]conn, error) {
	if len(creds) == 0 {
		return nil, status.Error(codes.InvalidArgument, "credentials required")
	}
	conns := make([]conn, 0, len(creds))
	for _, pc := range creds {
		var cred credential
		if json.Unmarshal(pc.GetData(), &cred) != nil || cred.Token == "" || cred.BotID <= 0 || !ValidPath(cred.Path) {
			return nil, status.Error(codes.InvalidArgument, "credential malformed")
		}
		conns = append(conns, conn{http: s.http, base: s.base, cred: cred})
	}
	return conns, nil
}

// pick returns the connection with the longest path prefix covering repo.
func pick(conns []conn, repo string) (conn, error) {
	if !ValidPath(repo) || !strings.Contains(repo, "/") {
		return conn{}, status.Error(codes.InvalidArgument, "expected a full GitLab project path")
	}
	var best conn
	for _, c := range conns {
		if c.covers(repo) && len(c.cred.Path) > len(best.cred.Path) {
			best = c
		}
	}
	if best.cred.Token == "" {
		return conn{}, status.Errorf(codes.FailedPrecondition, "no GitLab connection covers %s", repo)
	}
	return best, nil
}

func (s *Server) connectionFor(creds []*pb.Credential, repo string) (conn, error) {
	conns, err := s.connections(creds)
	if err != nil {
		return conn{}, err
	}
	return pick(conns, repo)
}

func (s *Server) BeginConnect(context.Context, *pb.BeginConnectRequest) (*pb.BeginConnectResponse, error) {
	return nil, status.Error(codes.Unimplemented, "GitLab connects by form; use CompleteConnect with fields")
}

// CompleteConnect checks that the token's user is at least Maintainer on the
// group or project and returns the stored credential. Errors name the field,
// never its value.
func (s *Server) CompleteConnect(ctx context.Context, req *pb.CompleteConnectRequest) (*pb.CompleteConnectResponse, error) {
	fields := req.GetFields()
	for _, f := range connectFields {
		if strings.TrimSpace(fields[f.GetName()]) == "" {
			return nil, status.Errorf(codes.InvalidArgument, "field %s is required", f.GetName())
		}
	}
	path := strings.Trim(strings.TrimSpace(fields["path"]), "/")
	if !ValidPath(path) {
		return nil, status.Error(codes.InvalidArgument, "field path: expected a GitLab group or project path")
	}
	c := conn{http: s.http, base: s.base, cred: credential{Token: strings.TrimSpace(fields["token"])}}

	var me struct {
		ID       int64
		Username string
	}
	if err := c.request(ctx, http.MethodGet, "/user", nil, nil, &me); err != nil {
		if errors.Is(err, apiError(http.StatusUnauthorized)) || errors.Is(err, apiError(http.StatusForbidden)) {
			return nil, status.Error(codes.InvalidArgument, "field token: token rejected")
		}
		return nil, grpcError(err)
	}
	if me.ID <= 0 || me.Username == "" {
		return nil, status.Error(codes.Unavailable, "GitLab did not return the token's user")
	}
	c.cred.BotID, c.cred.BotUsername = me.ID, me.Username

	if err := c.resolve(ctx, path); err != nil {
		return nil, err
	}
	var member struct {
		AccessLevel int `json:"access_level"`
	}
	err := c.request(ctx, http.MethodGet, "/"+c.cred.Kind+"s/"+strconv.FormatInt(c.cred.ID, 10)+"/members/all/"+strconv.FormatInt(me.ID, 10), nil, nil, &member)
	if err != nil && !errors.Is(err, apiError(http.StatusNotFound)) {
		return nil, grpcError(err)
	}
	if member.AccessLevel < maintainerAccess {
		return nil, status.Errorf(codes.PermissionDenied, "maintainer role required on %s", c.cred.Path)
	}
	data, _ := json.Marshal(c.cred) // plain struct: cannot fail
	return &pb.CompleteConnectResponse{
		Credential: &pb.Credential{Data: data},
		Label:      c.cred.Path,
		ExternalId: c.cred.Kind + ":" + strconv.FormatInt(c.cred.ID, 10),
	}, nil
}

// resolve finds path as a group, else as a project, and fills the
// credential's kind, id and canonical full path.
func (c *conn) resolve(ctx context.Context, path string) error {
	var group struct {
		ID       int64
		FullPath string `json:"full_path"`
	}
	err := c.request(ctx, http.MethodGet, "/groups/"+url.PathEscape(path), url.Values{"with_projects": {"false"}}, nil, &group)
	switch {
	case err == nil && (group.ID <= 0 || !ValidPath(group.FullPath)):
		return status.Error(codes.Unavailable, "GitLab returned an incomplete group")
	case err == nil:
		c.cred.Kind, c.cred.ID, c.cred.Path = "group", group.ID, group.FullPath
		return nil
	case !errors.Is(err, apiError(http.StatusNotFound)):
		return grpcError(err)
	}
	var project struct {
		ID       int64
		FullPath string `json:"path_with_namespace"`
	}
	err = c.request(ctx, http.MethodGet, projectPath(path), nil, nil, &project)
	if errors.Is(err, apiError(http.StatusNotFound)) {
		return status.Error(codes.InvalidArgument, "field path: no group or project with this path is visible to the token")
	}
	if err != nil {
		return grpcError(err)
	}
	if project.ID <= 0 || !ValidPath(project.FullPath) {
		return status.Error(codes.Unavailable, "GitLab returned an incomplete project")
	}
	c.cred.Kind, c.cred.ID, c.cred.Path = "project", project.ID, project.FullPath
	return nil
}

// RefreshCredential returns the static token unchanged; a revoked token shows
// up as UNAUTHENTICATED on the next call.
func (s *Server) RefreshCredential(_ context.Context, req *pb.RefreshCredentialRequest) (*pb.Credential, error) {
	return req.GetCredential(), nil
}

// Disconnect has nothing to revoke: the tenant owns the token.
func (s *Server) Disconnect(context.Context, *pb.DisconnectRequest) (*pb.DisconnectResponse, error) {
	return &pb.DisconnectResponse{}, nil
}

package adapter

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	pb "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	developerAccess = 30
	tokenName       = "orchivio"
	cleanupTimeout  = 15 * time.Second
)

// issuedToken is one entry of credential_id: what revocation needs, no secret.
type issuedToken struct {
	ProjectID int64 `json:"project_id"`
	TokenID   int64 `json:"token_id"`
}

func encodeCredentialID(tokens []issuedToken) string {
	data, _ := json.Marshal(tokens) // ints only: cannot fail
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeCredentialID(id string) ([]issuedToken, error) {
	data, err := base64.RawURLEncoding.DecodeString(id)
	var tokens []issuedToken
	if err != nil || json.Unmarshal(data, &tokens) != nil {
		return nil, status.Error(codes.InvalidArgument, "credential_id malformed")
	}
	return tokens, nil
}

// IssueRepoCredentials creates one Developer project access token per repo,
// valid until midnight UTC tomorrow. The credential_id carries the project
// and token ids, so revocation survives an adapter restart.
func (s *Server) IssueRepoCredentials(ctx context.Context, req *pb.IssueRepoCredentialsRequest) (*pb.RepoCredentials, error) {
	if len(req.GetRepos()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one repo is required")
	}
	conns, err := s.connections(req.GetCredentials())
	if err != nil {
		return nil, err
	}
	expiry := s.now().UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
	scopes := []string{"read_repository"}
	if req.GetWrite() {
		scopes = append(scopes, "write_repository")
	}
	result := &pb.RepoCredentials{ExpiresAt: timestamppb.New(expiry), CloneUrls: make(map[string]string), Tokens: make(map[string]*pb.RepoToken)}
	var issued []issuedToken
	for _, repo := range req.GetRepos() {
		if result.Tokens[repo] != nil {
			continue
		}
		token, id, err := s.createToken(ctx, conns, repo, scopes, expiry)
		if id.TokenID > 0 {
			issued = append(issued, id)
		}
		if err != nil {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
			if rollbackErr := s.revoke(cleanup, conns, issued); rollbackErr != nil {
				slog.Warn("GitLab token rollback failed", "error", rollbackErr)
			}
			cancel()
			return nil, err
		}
		result.Tokens[repo] = token
		result.CloneUrls[repo] = strings.TrimRight(s.base.String(), "/") + "/" + repo + ".git"
	}
	result.CredentialId = encodeCredentialID(issued)
	return result, nil
}

func (s *Server) createToken(ctx context.Context, conns []conn, repo string, scopes []string, expiry time.Time) (*pb.RepoToken, issuedToken, error) {
	c, err := pick(conns, repo)
	if err != nil {
		return nil, issuedToken{}, err
	}
	var project struct{ ID int64 }
	if err := c.request(ctx, http.MethodGet, projectPath(repo), nil, nil, &project); err != nil {
		return nil, issuedToken{}, grpcError(err)
	}
	if project.ID <= 0 {
		return nil, issuedToken{}, status.Error(codes.Unavailable, "GitLab returned an invalid project id")
	}
	body := map[string]any{"name": tokenName, "scopes": scopes, "access_level": developerAccess, "expires_at": expiry.Format(time.DateOnly)}
	var created struct {
		ID       int64
		Token    string
		Username string
	}
	err = c.request(ctx, http.MethodPost, fmt.Sprintf("/projects/%d/access_tokens", project.ID), nil, body, &created)
	id := issuedToken{ProjectID: project.ID, TokenID: created.ID}
	switch {
	case errors.Is(err, apiError(http.StatusForbidden)), errors.Is(err, apiError(http.StatusNotFound)):
		return nil, id, status.Errorf(codes.FailedPrecondition, "Project Access Tokens nicht verfügbar für %s", repo)
	case err != nil:
		return nil, id, grpcError(err)
	case created.ID <= 0 || created.Token == "":
		return nil, id, status.Error(codes.Unavailable, "GitLab returned an incomplete project access token")
	}
	username := created.Username
	if username == "" {
		username = "oauth2"
	}
	return &pb.RepoToken{Username: username, Token: created.Token, ExpiresAt: timestamppb.New(expiry)}, id, nil
}

// RevokeRepoCredentials deletes every token named in credential_id; failures
// are collected and returned as one error. Expiry tomorrow is the fallback.
func (s *Server) RevokeRepoCredentials(ctx context.Context, req *pb.RevokeRepoCredentialsRequest) (*pb.RevokeRepoCredentialsResponse, error) {
	if req.GetCredentialId() == "" {
		return &pb.RevokeRepoCredentialsResponse{}, nil
	}
	tokens, err := decodeCredentialID(req.GetCredentialId())
	if err != nil {
		return nil, err
	}
	conns, err := s.connections(req.GetCredentials())
	if err != nil {
		return nil, err
	}
	if err := s.revoke(ctx, conns, tokens); err != nil {
		return nil, grpcError(err)
	}
	return &pb.RevokeRepoCredentialsResponse{}, nil
}

// revoke deletes each token. credential_id names no path, so the matching
// connection is not known: a project connection with that id goes first,
// then the others; the first success wins and 404 everywhere means gone.
// ponytail: one DELETE per connection in the worst case; add the path to
// credential_id if tenants have many connections.
func (s *Server) revoke(ctx context.Context, conns []conn, tokens []issuedToken) error {
	var errs []error
	for _, token := range tokens {
		if err := revokeOne(ctx, conns, token); err != nil {
			errs = append(errs, fmt.Errorf("revoke token %d of project %d: %w", token.TokenID, token.ProjectID, err))
		}
	}
	return errors.Join(errs...)
}

func revokeOne(ctx context.Context, conns []conn, token issuedToken) error {
	ordered := make([]conn, 0, len(conns))
	for _, c := range conns {
		if c.cred.Kind == "project" && c.cred.ID == token.ProjectID {
			ordered = append([]conn{c}, ordered...)
		} else {
			ordered = append(ordered, c)
		}
	}
	path := fmt.Sprintf("/projects/%d/access_tokens/%d", token.ProjectID, token.TokenID)
	var failure error
	for _, c := range ordered {
		err := c.request(ctx, http.MethodDelete, path, nil, nil, nil)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, apiError(http.StatusNotFound)):
		case failure == nil:
			failure = err
		}
	}
	return failure
}

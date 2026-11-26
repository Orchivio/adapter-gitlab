package adapter

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type tokenRequest struct {
	Name        string   `json:"name"`
	Scopes      []string `json:"scopes"`
	AccessLevel int      `json:"access_level"`
	ExpiresAt   string   `json:"expires_at"`
}

// tokenFake serves projects g/a (id 1) and g/b (id 2); access token creation
// answers 404 for failProject ("" = none) and every request is recorded.
type tokenFake struct {
	*httptest.Server
	mu       sync.Mutex
	created  map[string]tokenRequest
	requests []string
}

func newTokenFake(t *testing.T, failProject string) *tokenFake {
	t.Helper()
	f := &tokenFake{created: make(map[string]tokenRequest)}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("PRIVATE-TOKEN") != fakeToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		path := r.URL.EscapedPath()
		f.mu.Lock()
		defer f.mu.Unlock()
		f.requests = append(f.requests, r.Method+" "+path)
		switch {
		case r.Method == http.MethodGet && path == "/api/v4/projects/g%2Fa":
			fmt.Fprint(w, `{"id":1}`)
		case r.Method == http.MethodGet && path == "/api/v4/projects/g%2Fb":
			fmt.Fprint(w, `{"id":2}`)
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/access_tokens"):
			project := strings.Split(path, "/")[4]
			if project == failProject {
				http.NotFound(w, r)
				return
			}
			var body tokenRequest
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.created[project] = body
			fmt.Fprintf(w, `{"id":1%s,"token":"pat-%s"}`, project, project)
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *tokenFake) createdTokens() map[string]tokenRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.created
}

func (f *tokenFake) deletes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, r := range f.requests {
		if strings.HasPrefix(r, http.MethodDelete) {
			out = append(out, r)
		}
	}
	return out
}

func issue(t *testing.T, s *Server, write bool) (*pb.RepoCredentials, error) {
	t.Helper()
	return s.IssueRepoCredentials(t.Context(), &pb.IssueRepoCredentialsRequest{Repos: []string{"g/a", "g/b"}, Write: write,
		Credentials: []*pb.Credential{groupCredential(t, "g", fakeToken)}})
}

func TestIssueTokensPerRepo(t *testing.T) {
	f := newTokenFake(t, "")
	s := newServer(t, f.URL)
	tomorrow := time.Date(2026, 11, 27, 0, 0, 0, 0, time.UTC)

	creds, err := issue(t, s, true)

	if err != nil {
		t.Fatal(err)
	}
	want := tokenRequest{Name: "orchivio", Scopes: []string{"read_repository", "write_repository"}, AccessLevel: 30, ExpiresAt: "2026-11-27"}
	if !reflect.DeepEqual(f.createdTokens(), map[string]tokenRequest{"1": want, "2": want}) {
		t.Fatalf("created = %+v", f.createdTokens())
	}
	a, b := creds.GetTokens()["g/a"], creds.GetTokens()["g/b"]
	if a.GetToken() != "pat-1" || b.GetToken() != "pat-2" || a.GetUsername() != "oauth2" || !a.GetExpiresAt().AsTime().Equal(tomorrow) {
		t.Fatalf("tokens = %v", creds.GetTokens())
	}
	if !creds.GetExpiresAt().AsTime().Equal(tomorrow) || creds.GetToken() != "" {
		t.Fatalf("top level = %v", creds)
	}
	if creds.GetCloneUrls()["g/a"] != f.URL+"/g/a.git" || creds.GetCloneUrls()["g/b"] != f.URL+"/g/b.git" {
		t.Fatalf("clone urls = %v", creds.GetCloneUrls())
	}
	if creds.GetCredentialId() == "" || strings.Contains(creds.GetCredentialId(), "pat-") || strings.Contains(creds.GetCredentialId(), fakeToken) {
		t.Fatalf("credential_id = %q", creds.GetCredentialId())
	}
}

func TestIssueReadOnlyTokenHasReadScope(t *testing.T) {
	f := newTokenFake(t, "")
	s := newServer(t, f.URL)

	_, err := issue(t, s, false)

	if err != nil || !reflect.DeepEqual(f.createdTokens()["1"].Scopes, []string{"read_repository"}) {
		t.Fatalf("created = %+v, %v", f.createdTokens(), err)
	}
}

func TestRevokeFromCredentialIDAfterRestart(t *testing.T) {
	f := newTokenFake(t, "")
	creds, err := issue(t, newServer(t, f.URL), true)
	if err != nil {
		t.Fatal(err)
	}
	restarted := newServer(t, f.URL)

	_, err = restarted.RevokeRepoCredentials(t.Context(), &pb.RevokeRepoCredentialsRequest{CredentialId: creds.GetCredentialId(),
		Credentials: []*pb.Credential{groupCredential(t, "g", fakeToken)}})

	want := []string{"DELETE /api/v4/projects/1/access_tokens/11", "DELETE /api/v4/projects/2/access_tokens/12"}
	if err != nil || !reflect.DeepEqual(f.deletes(), want) {
		t.Fatalf("revoke = %v, deletes %v", err, f.deletes())
	}
}

func TestRevokeRejectsMalformedCredentialID(t *testing.T) {
	s := newServer(t, "https://gitlab.example.test")

	_, err := s.RevokeRepoCredentials(t.Context(), &pb.RevokeRepoCredentialsRequest{CredentialId: "not base64!",
		Credentials: []*pb.Credential{groupCredential(t, "g", fakeToken)}})

	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("revoke = %v; want InvalidArgument", err)
	}
}

func TestPremiumMissingIsFailedPrecondition(t *testing.T) {
	f := newTokenFake(t, "1")
	s := newServer(t, f.URL)

	_, err := issue(t, s, true)

	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "Project Access Tokens nicht verfügbar für g/a") {
		t.Fatalf("issue = %v; want FailedPrecondition naming g/a", err)
	}
}

func TestFailedIssueRevokesCreatedTokens(t *testing.T) {
	f := newTokenFake(t, "2")
	s := newServer(t, f.URL)

	_, err := issue(t, s, true)

	if status.Code(err) != codes.FailedPrecondition || !reflect.DeepEqual(f.deletes(), []string{"DELETE /api/v4/projects/1/access_tokens/11"}) {
		t.Fatalf("issue = %v, deletes %v", err, f.deletes())
	}
}

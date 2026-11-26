package adapter

import (
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const fakeToken = "glpat-fake-token"

var fixedNow = time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC)

func newServer(t *testing.T, base string) *Server {
	t.Helper()
	s, err := New(Config{URL: base, PollInterval: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return fixedNow }
	t.Cleanup(s.Close)
	return s
}

func credentialOf(t *testing.T, c credential) *pb.Credential {
	t.Helper()
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return &pb.Credential{Data: data}
}

func groupCredential(t *testing.T, path, token string) *pb.Credential {
	return credentialOf(t, credential{Path: path, ID: 9, Kind: "group", Token: token, BotID: 5, BotUsername: "orchivio-bot"})
}

func TestNewRequiresURLAndInterval(t *testing.T) {
	for _, cfg := range []Config{
		{PollInterval: time.Second},
		{URL: "https://user:pw@gitlab.example.test", PollInterval: time.Second},
		{URL: "https://gitlab.example.test"},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("New(%+v) succeeded", cfg)
		}
	}
}

func TestCapabilitiesListFormFields(t *testing.T) {
	s := newServer(t, "https://gitlab.example.test")
	caps, err := s.GetCapabilities(t.Context(), &pb.GetCapabilitiesRequest{})
	if err != nil || caps.GetAdapterName() != "gitlab" || !caps.GetSupportsReviewEvents() || caps.GetSupportsWebhooks() {
		t.Fatalf("caps = %v, %v", caps, err)
	}
	var got []string
	for _, f := range caps.GetConnectFields() {
		got = append(got, fmt.Sprintf("%s:%v:%v", f.GetName(), f.GetRequired(), f.GetSecret()))
	}
	if !reflect.DeepEqual(got, []string{"path:true:false", "token:true:true"}) {
		t.Fatalf("fields = %v", got)
	}
}

func TestRepoPicksLongestPrefixConnection(t *testing.T) {
	tokens := make(chan string, 4)
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokens <- r.URL.EscapedPath() + " " + r.Header.Get("PRIVATE-TOKEN")
		fmt.Fprint(w, `{"iid":3,"web_url":"https://gitlab.example.test/mr/3"}`)
	}))
	t.Cleanup(fake.Close)
	s := newServer(t, fake.URL)
	creds := []*pb.Credential{groupCredential(t, "g/special", "tok-special"), groupCredential(t, "g", "tok-g")}
	open := func(repo string) error {
		_, err := s.OpenPullRequest(t.Context(), &pb.OpenPullRequestRequest{Repo: repo, Base: "main", Head: "orchivio/x", Title: "T", Credentials: creds})
		return err
	}

	specialErr, siblingErr, foreignErr := open("g/special/p"), open("g/specialist/p"), open("other/p")

	if specialErr != nil || siblingErr != nil {
		t.Fatalf("open = %v, %v", specialErr, siblingErr)
	}
	if got := <-tokens; got != "/api/v4/projects/g%2Fspecial%2Fp/merge_requests tok-special" {
		t.Fatalf("g/special/p used %q", got)
	}
	if got := <-tokens; got != "/api/v4/projects/g%2Fspecialist%2Fp/merge_requests tok-g" {
		t.Fatalf("g/specialist/p used %q", got)
	}
	if status.Code(foreignErr) != codes.FailedPrecondition || !strings.Contains(foreignErr.Error(), "other/p") {
		t.Fatalf("other/p = %v; want FailedPrecondition", foreignErr)
	}
}

func TestOpenPullRequestSendsOnlyMRFields(t *testing.T) {
	bodies := make(chan map[string]any, 1)
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		bodies <- body
		fmt.Fprint(w, `{"iid":18,"web_url":"https://gitlab.example.test/g/p/-/merge_requests/18"}`)
	}))
	t.Cleanup(fake.Close)
	s := newServer(t, fake.URL)

	mr, err := s.OpenPullRequest(t.Context(), &pb.OpenPullRequestRequest{Repo: "g/p", Base: "main", Head: "orchivio/abc-1", Title: "Title", Body: "Body",
		Credentials: []*pb.Credential{groupCredential(t, "g", fakeToken)}})

	if err != nil || mr.GetNumber() != 18 || mr.GetUrl() != "https://gitlab.example.test/g/p/-/merge_requests/18" {
		t.Fatalf("OpenPullRequest = %v, %v", mr, err)
	}
	want := map[string]any{"source_branch": "orchivio/abc-1", "target_branch": "main", "title": "Title", "description": "Body"}
	if got := <-bodies; !reflect.DeepEqual(got, want) {
		t.Fatalf("body = %v", got)
	}
}

func TestUnknownRepoIsNotFound(t *testing.T) {
	fake := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(fake.Close)
	s := newServer(t, fake.URL)

	_, err := s.OpenPullRequest(t.Context(), &pb.OpenPullRequestRequest{Repo: "g/missing", Base: "main", Head: "orchivio/x", Title: "T",
		Credentials: []*pb.Credential{groupCredential(t, "g", fakeToken)}})

	if status.Code(err) != codes.NotFound {
		t.Fatalf("OpenPullRequest = %v; want NotFound", err)
	}
}

func TestRejectedTokenIsUnauthenticated(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(fake.Close)
	s := newServer(t, fake.URL)
	creds := []*pb.Credential{groupCredential(t, "g", fakeToken)}
	ctx := t.Context()

	_, prErr := s.OpenPullRequest(ctx, &pb.OpenPullRequestRequest{Repo: "g/p", Base: "main", Head: "orchivio/x", Title: "T", Credentials: creds})
	_, issueErr := s.IssueRepoCredentials(ctx, &pb.IssueRepoCredentialsRequest{Repos: []string{"g/p"}, Credentials: creds})
	_, accessErr := s.CheckAccess(ctx, &pb.CheckAccessRequest{Items: []string{"g/p"}, Credentials: creds})

	for name, err := range map[string]error{"OpenPullRequest": prErr, "IssueRepoCredentials": issueErr, "CheckAccess": accessErr} {
		if status.Code(err) != codes.Unauthenticated || strings.Contains(err.Error(), fakeToken) {
			t.Errorf("%s: %v; want Unauthenticated without token", name, err)
		}
	}
}

func TestCheckAccessDeniesUncoveredAndMissingRepos(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() == "/api/v4/projects/g%2Fok" {
			fmt.Fprint(w, `{"id":1}`)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(fake.Close)
	s := newServer(t, fake.URL)

	resp, err := s.CheckAccess(t.Context(), &pb.CheckAccessRequest{Items: []string{"g/ok", "g/missing", "other/p"},
		Credentials: []*pb.Credential{groupCredential(t, "g", fakeToken)}})

	if err != nil || !reflect.DeepEqual(resp.GetDenied(), []string{"g/missing", "other/p"}) {
		t.Fatalf("CheckAccess = %v, %v", resp, err)
	}
}

func TestMalformedCredentialIsNotEchoed(t *testing.T) {
	s := newServer(t, "https://gitlab.example.test")

	_, err := s.OpenPullRequest(t.Context(), &pb.OpenPullRequestRequest{Repo: "g/p", Base: "main", Head: "h", Title: "T",
		Credentials: []*pb.Credential{{Data: []byte(`{"token":"s3cr3t"`)}}})

	if status.Code(err) != codes.InvalidArgument || strings.Contains(err.Error(), "s3cr3t") {
		t.Fatalf("OpenPullRequest = %v", err)
	}
}

func TestTLSWithCustomCA(t *testing.T) {
	fake := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"iid":1,"web_url":"https://gitlab.example.test/mr/1"}`)
	}))
	t.Cleanup(fake.Close)
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: fake.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSL_CERT_FILE", caFile)
	s := newServer(t, fake.URL)

	mr, err := s.OpenPullRequest(t.Context(), &pb.OpenPullRequestRequest{Repo: "g/p", Base: "main", Head: "orchivio/x", Title: "T",
		Credentials: []*pb.Credential{groupCredential(t, "g", fakeToken)}})

	if err != nil || mr.GetNumber() != 1 {
		t.Fatalf("OpenPullRequest = %v, %v", mr, err)
	}
}

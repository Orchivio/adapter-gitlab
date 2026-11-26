package adapter

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gitlabConnectFake serves /user, a group "g" (id 9) and a project "g/p"
// (id 42); the bot's role on both is accessLevel.
func gitlabConnectFake(t *testing.T, accessLevel int) *httptest.Server {
	t.Helper()
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("PRIVATE-TOKEN") != fakeToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.EscapedPath() {
		case "/api/v4/user":
			fmt.Fprint(w, `{"id":5,"username":"orchivio-bot"}`)
		case "/api/v4/groups/g":
			fmt.Fprint(w, `{"id":9,"full_path":"g"}`)
		case "/api/v4/projects/g%2Fp":
			fmt.Fprint(w, `{"id":42,"path_with_namespace":"g/p"}`)
		case "/api/v4/groups/9/members/all/5", "/api/v4/projects/42/members/all/5":
			fmt.Fprintf(w, `{"id":5,"access_level":%d}`, accessLevel)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fake.Close)
	return fake
}

func connect(t *testing.T, s *Server, path, token string) (*pb.CompleteConnectResponse, error) {
	t.Helper()
	return s.CompleteConnect(t.Context(), &pb.CompleteConnectRequest{Fields: map[string]string{"path": path, "token": token}})
}

func TestConnectRequiresMaintainer(t *testing.T) {
	s := newServer(t, gitlabConnectFake(t, 30).URL)

	_, err := connect(t, s, "g", fakeToken)

	if status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), "maintainer role required on g") || strings.Contains(err.Error(), fakeToken) {
		t.Fatalf("CompleteConnect = %v; want PermissionDenied", err)
	}
}

func TestConnectGroupReturnsCredential(t *testing.T) {
	s := newServer(t, gitlabConnectFake(t, 40).URL)

	resp, err := connect(t, s, "g", fakeToken)

	if err != nil || resp.GetExternalId() != "group:9" || resp.GetLabel() != "g" {
		t.Fatalf("CompleteConnect = %v, %v", resp, err)
	}
	var got credential
	if err := json.Unmarshal(resp.GetCredential().GetData(), &got); err != nil {
		t.Fatal(err)
	}
	want := credential{Path: "g", ID: 9, Kind: "group", Token: fakeToken, BotID: 5, BotUsername: "orchivio-bot"}
	if got != want {
		t.Fatalf("credential = %+v", got)
	}
}

func TestConnectProjectReturnsProjectIdentity(t *testing.T) {
	s := newServer(t, gitlabConnectFake(t, 50).URL)

	resp, err := connect(t, s, "g/p", fakeToken)

	if err != nil || resp.GetExternalId() != "project:42" || resp.GetLabel() != "g/p" {
		t.Fatalf("CompleteConnect = %v, %v", resp, err)
	}
}

func TestConnectRejectedTokenIsInvalidArgument(t *testing.T) {
	s := newServer(t, gitlabConnectFake(t, 40).URL)

	_, err := connect(t, s, "g", "s3cr3t")

	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "token rejected") || strings.Contains(err.Error(), "s3cr3t") {
		t.Fatalf("CompleteConnect = %v; want InvalidArgument token rejected", err)
	}
}

func TestConnectValidatesFieldsWithoutEchoing(t *testing.T) {
	s := newServer(t, gitlabConnectFake(t, 40).URL)

	_, missingErr := connect(t, s, "g", "")
	_, badPathErr := connect(t, s, "../evil?x", fakeToken)
	_, unknownErr := connect(t, s, "nope", fakeToken)

	if status.Code(missingErr) != codes.InvalidArgument || !strings.Contains(missingErr.Error(), "token") {
		t.Errorf("missing token: %v", missingErr)
	}
	if status.Code(badPathErr) != codes.InvalidArgument || strings.Contains(badPathErr.Error(), "evil") {
		t.Errorf("bad path: %v", badPathErr)
	}
	if status.Code(unknownErr) != codes.InvalidArgument || !strings.Contains(unknownErr.Error(), "field path") {
		t.Errorf("unknown path: %v", unknownErr)
	}
}

func TestFormOnlyConnectionLifecycle(t *testing.T) {
	s := newServer(t, "https://gitlab.example.test")
	cred := groupCredential(t, "g", fakeToken)

	_, beginErr := s.BeginConnect(t.Context(), &pb.BeginConnectRequest{})
	refreshed, refreshErr := s.RefreshCredential(t.Context(), &pb.RefreshCredentialRequest{Credential: cred})
	_, disconnectErr := s.Disconnect(t.Context(), &pb.DisconnectRequest{Credential: cred})

	if status.Code(beginErr) != codes.Unimplemented {
		t.Errorf("BeginConnect = %v", beginErr)
	}
	if refreshErr != nil || string(refreshed.GetData()) != string(cred.GetData()) {
		t.Errorf("RefreshCredential = %v, %v", refreshed, refreshErr)
	}
	if disconnectErr != nil {
		t.Errorf("Disconnect = %v", disconnectErr)
	}
}

# Orchivio GitLab adapter

A standalone `plugin.v1` code host adapter for GitLab Self-Managed and GitLab.com. Run one process per GitLab instance. Tenants connect their own groups or projects through a form. The adapter opens merge requests, polls merge request comments, and issues short-lived project access tokens for clone and push.

## Configuration

| Variable | Value |
| --- | --- |
| `GITLAB_URL` | Required. The GitLab base URL, including any context path, e.g. `https://gitlab.com` |
| `GITLAB_POLL_INTERVAL` | How often merge request notes are polled, default `30s` |
| `ORCHIVIO_GRPC_ADDR` | The gRPC listen address, default `127.0.0.1:7401` |

The process holds no tenant secrets. Each call brings the tenant's connections from the orchestrator.

## Connecting a tenant

The adapter has no OAuth flow, so `BeginConnect` returns `Unimplemented`. `GetCapabilities` lists the form fields:

- `path` (required): a group or project path, e.g. `acme` or `acme/backend/api`
- `token` (required, secret): a group, project or personal access token with scope `api` and the Maintainer role on that path

`CompleteConnect` checks the token with `GET /user`. It then looks up the path as a group, or else as a project, and requires the token's user to have at least Maintainer access (level 40). The result has `external_id` `group:<id>` or `project:<id>`, and the label is the full path. That user is the bot: `@<username>` in a comment sets `mentions_bot`, and the poller drops the bot's own notes.

A tenant can have any number of connections. For each repo the adapter uses the connection with the longest matching path prefix.

## Clone and push tokens

`IssueRepoCredentials` creates one project access token per repo through `POST /projects/:id/access_tokens`. Each token gets the Developer role, scope `read_repository` (plus `write_repository` for push), and expires at midnight UTC tomorrow. `credential_id` encodes only project and token ids. `RevokeRepoCredentials` uses it to delete the tokens, even after an adapter restart. Project access tokens need a paid tier on GitLab.com. If an instance cannot create them, the call fails with `FAILED_PRECONDITION` and names the repo. [Project access token API](https://docs.gitlab.com/api/project_access_tokens/)

## Review comments

`StreamEvents` polls each requested repo for open merge requests whose source branch starts with `orchivio/` and whose source project is the repo itself, so forks are skipped. It then reads their notes since the cursor, with a two-minute overlap, and dedups by `note:<id>`. System notes and the bot's own notes are dropped. If one repo fails, the error is logged and the other repos keep streaming.

## Network

Outbound HTTP honours `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY`. `SSL_CERT_FILE` adds a custom CA (for example a self-signed GitLab) to the system roots on every platform. `SSL_CERT_DIR` works on Linux.

## Build and test

Until the contract `v0.4.0` tag is published, `go.mod` still names `v0.3.0`, and the build uses the parent directory's `go.work`, which includes `../orchestrator/contract`.

```sh
go vet ./... && go test -race ./...
go build -o /tmp/orchivio-gitlab-adapter ./cmd/orchivio-gitlab-adapter
```

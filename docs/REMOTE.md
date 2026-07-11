# Remote protocol

FVS repositories sync over a small content-addressed HTTP API. `fvs2 push`
uploads a branch head, `fvs2 pull` downloads one; `fvs2 serve` is the
reference server, backing a remote with a plain directory.

## Model

A remote stores the same three things a local repo does:

- **blocks**, content-addressed by BLAKE3; global to the remote, so identical
  content pushed by different repositories or different accounts is stored
  once, whoever uploaded it first.
- **states**, the commit documents, by id.
- **refs**, named pointers to state ids, namespaced per account and updated
  with compare-and-swap.

Push is: ask which blocks are missing, upload those in compressed batches,
upload the state document, swap the ref. Pull is the reverse; the working
tree is never touched, `restore` materializes the state afterwards.

Refs have no ancestry (states are independent), so divergence is detected by
knowledge: pushing over a remote state this repository has never seen fails
and requires `--force` or a pull first. Concurrent pushes are serialized by
the compare-and-swap: the loser gets a conflict, never a silent overwrite.

## Accounts, teams and quotas

A server runs either open (no auth), with a single admin token (`--token`),
or with per-account access from a file the server owns (`--accounts
accounts.json`):

```json
[
  { "name": "mirko", "token": "tok-1", "admin": true },
  { "name": "deck",  "token": "tok-2", "quota_bytes": 10737418240 },
  { "name": "alice", "token": "tok-3", "teams": ["acme"] }
]
```

Each account authenticates with `Authorization: Bearer <token>`. Refs live
under a namespace: an account's own name by default, or a **team** it belongs
to, selected with the `X-Fvs-Namespace` header (the `--namespace` flag on
`fvs2 remote add`). Team members share the same refs; non-members are refused.
Blocks stay shared across everyone: an account's quota counts the bytes of
blocks it was the first to upload, so content the store already has is free.
Usage is persisted in `usage.json` next to the store.

Accounts are managed **at runtime**, without a restart, by an admin:

```bash
fvs2 remote user add deck --token tok-2 --quota 10737418240
fvs2 remote user add alice --token tok-3 --teams acme
fvs2 remote user list
fvs2 remote user remove deck
```

Changes persist to the `--accounts` file. The admin endpoints are also
available directly under `/v1/admin/accounts`.

## Running several instances

Every mutation that needs coordination, the ref compare-and-swap, the quota
counter, account changes and gc, serializes through file locks on the storage
root, not process memory. Several `fvs2 serve` processes (on one machine, or
on several machines sharing the root over a filesystem with working `flock`,
e.g. local disk or NFSv4) can therefore serve the same remote behind a load
balancer: concurrent pushes through different instances still resolve to one
winner and one clean conflict, quotas stay consistent, and an account added on
one node authenticates on the others immediately.

## Transport security

Pass `--tls-cert` and `--tls-key` to serve HTTPS directly; without them the
server speaks plain HTTP (put it behind a TLS reverse proxy, or keep it on
localhost). Browser clients (the web UI) need `--cors-origin` set to their
origin; preflight `OPTIONS` requests pass without auth.

## Block storage

Blocks live on local disk by default. Point them at any S3-compatible object
store (AWS S3, MinIO, Cloudflare R2) with `--s3-endpoint`, `--s3-bucket` and
credentials; states and refs stay local, where compare-and-swap is cheap.

## Observability

`/metrics` serves Prometheus counters (requests, blocks added, bytes moved,
rate-limited and quota-rejected requests, uptime) with no auth. `--audit
<file>` appends one JSON line per mutating request (account, method, path,
status). `--rate` caps requests per second per account (token bucket, with
`--burst`); over-limit requests get `429`.

## Transfers

Blocks travel as length-prefixed frames (4-byte big-endian length + payload,
16 MiB frame cap). Uploads are batched (about 8 MiB per request) and
gzip-compressed on the wire; downloads stream every requested block in one
response, gzip-compressed when the client accepts it. Integrity is enforced
on both sides: the server refuses frames whose content does not hash to a
proper id, and the client re-hashes every downloaded block before storing it.

## Endpoints

All under `/v1/`.

| Method | Path | Body | Response |
|---|---|---|---|
| POST | `/v1/blocks/check` | `{"blocks": ["<id>", ...]}` | `{"missing": [...]}` |
| POST | `/v1/blocks/batch` | block frames | `{"added": n}`; 413 over quota |
| POST | `/v1/blocks/fetch` | `{"blocks": [...]}` | block frames, in request order |
| PUT | `/v1/blocks/<id>` | raw bytes | 201; 400 if the content does not hash to `<id>` |
| GET | `/v1/blocks/<id>` | | raw bytes |
| PUT | `/v1/states/<id>` | state JSON | 201 |
| GET | `/v1/states/<id>` | | state JSON |
| GET | `/v1/refs/<name>` | | `{"id": "<state id>"}`; 404 if absent |
| PUT | `/v1/refs/<name>` | `{"id": "...", "old": "..."}` | 204; 409 with the current id when `old` does not match (empty `old` means "must not exist") |
| DELETE | `/v1/refs/<name>` | | 204 |
| GET | `/v1/whoami` | | calling account: name, teams, admin, quota and usage |
| GET | `/v1/refs` | | every ref in the namespace, with its state id |
| GET | `/v1/admin/accounts` | | account list (tokens redacted); admin only |
| POST | `/v1/admin/accounts` | account JSON | 201; admin only |
| DELETE | `/v1/admin/accounts/<name>` | | 204; admin only |
| POST | `/v1/gc?grace_seconds=N` | | removal counts; admin only |
| GET | `/metrics` | | Prometheus counters; no auth |

Ref requests carry an optional `X-Fvs-Namespace` header to target a team.

## Garbage collection

`fvs2 remote gc` (admin) removes every state no ref reaches anymore, and
every block no surviving state references. Objects newer than the grace
window (default one hour) are always kept, so a push in flight, blocks
uploaded but ref not moved yet, can never be collected.

## Usage

```bash
# on the server: HTTPS, runtime-managed accounts, S3 blocks, audit + metrics
fvs2 serve --root /srv/fvs --addr 0.0.0.0:8040 \
    --accounts /etc/fvs/accounts.json \
    --tls-cert /etc/fvs/cert.pem --tls-key /etc/fvs/key.pem \
    --s3-endpoint s3.example.org --s3-bucket fvs \
    --s3-access-key KEY --s3-secret-key SECRET --s3-ssl \
    --audit /var/log/fvs/audit.log --rate 100

# on each machine
fvs2 remote add origin https://example.org:8040 --token tok-2
fvs2 push                 # upload the current branch head
fvs2 pull                 # fetch it elsewhere
fvs2 restore -s <state>   # materialize it

# a shared team namespace
fvs2 remote add team https://example.org:8040 --token tok-3 --namespace acme
fvs2 push --remote team

# housekeeping (admin account)
fvs2 remote gc --grace 3600
```

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

## Accounts and quotas

A server runs either open (no auth), with a single admin token (`--token`),
or with per-user accounts (`--users accounts.json`):

```json
[
  { "name": "mirko", "token": "tok-1", "admin": true },
  { "name": "deck",  "token": "tok-2", "quota_bytes": 10737418240 }
]
```

Each account authenticates with `Authorization: Bearer <token>` and sees only
its own refs. Blocks stay shared: an account's quota counts the bytes of
blocks it was the first to upload, so content the store already has is free.
Usage is persisted in `usage.json` next to the store.

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
| POST | `/v1/gc?grace_seconds=N` | | removal counts; admin only |

## Garbage collection

`fvs2 remote gc` (admin) removes every state no ref reaches anymore, and
every block no surviving state references. Objects newer than the grace
window (default one hour) are always kept, so a push in flight, blocks
uploaded but ref not moved yet, can never be collected.

## Usage

```bash
# on the server (a directory is the whole backend)
fvs2 serve --root /srv/fvs --addr 0.0.0.0:8040 --users /etc/fvs/accounts.json

# on each machine
fvs2 remote add origin https://example.org:8040 --token tok-2
fvs2 push                 # upload the current branch head
fvs2 pull                 # fetch it elsewhere
fvs2 restore -s <state>   # materialize it

# housekeeping (admin account)
fvs2 remote gc --grace 3600
```

Serve plain HTTP: put it behind a TLS reverse proxy for anything beyond
localhost.

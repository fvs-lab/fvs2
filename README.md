# fvs2 (standalone CLI)

Standalone CLI for FVS v2.

## Build (static)

```bash
CGO_ENABLED=0 go build -o ./bin/fvs2 ./cmd/fvs2
./bin/fvs2 --help
```

## Global flags

- `--path` (default: `.`) target directory (repo root)

## Commands

- `init` (flag: `--block-size`)
- `commit` (flag: `-m/--message`)
- `states`
- `restore` (flags: `-s/--state`, `--to`)
- `branch list|create|delete`
- `checkout <branch|commit>`
- `status`
- `mount` / `unmount` (IPC client to `fvs2d`; requires daemon-side IPC server)

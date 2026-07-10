# Layered environments

An environment composes several FVS repositories into one mounted stack: a
base runtime, shared dependencies, an application, each versioned
independently. A manifest declares the layers, a lockfile pins them to exact
states, and a plan feeds fvs2d's mount API. The point is reproducibility: the
same lockfile mounts byte-identical layers on every machine, which is what
makes a team or a CI fleet share one golden stack.

## Manifest

`env.json` lists the layers, lowest first (higher layers shadow lower ones):

```json
{
  "name": "myapp-env",
  "layers": [
    { "name": "runtime", "repo": "runtime" },
    { "name": "deps",    "repo": "deps", "branch": "stable" },
    { "name": "app",     "repo": "app",  "state": "cf20c17a" }
  ],
  "mount": "/opt/myapp",
  "upper": "/var/lib/myapp/upper"
}
```

Each layer pins to a `state` (id or prefix), a `branch` head, or the repo HEAD
when neither is set. Repo paths are relative to the manifest's directory. A
layer may add a `pull` block (`{ "remote": "...", "branch": "..." }`) to refresh
its repository from a remote before pinning.

## Commands

```bash
fvs2 env lock     # resolve the manifest into env.lock.json (pins every layer)
fvs2 env verify   # check that every pinned state is present locally
fvs2 env sync     # fetch pinned states from their remotes (consumer side)
fvs2 env plan     # print the ordered mount plan (repo + state per layer)
```

The plan lists `repo@state` per layer in mount order; hand those to fvs2d
(`CreateMount`, or the Virgo client's `mount --layer repo@state`) to mount the
composed stack, with the manifest's `upper` as the writable layer.

## The B2B flow

1. A publisher builds the golden layers, pushes them to a remote, and runs
   `env lock`. The lockfile pins each layer to a concrete state.
2. The lockfile ships with the project (commit it next to the manifest).
3. Any machine with only the lockfile runs `env sync` to fetch the exact
   pinned states from the remote, then mounts the plan. Because layers are
   content-addressed, the shared base is downloaded once and deduplicated
   across every environment that builds on it.

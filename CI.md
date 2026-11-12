# Validation and downloads

Every main-branch push, pull request and `v*` tag runs native platform builds,
tests, vet and EUProvGuard. Linux amd64, 386 and arm64 and macOS arm64 are built.
Targets are declared in `.github/platforms.json`. The daemon enables macOS
only with its native FSKit adapter; its earlier Linux-only source cannot build
a macOS filesystem. Removing a declared backend makes the build fail.
The 64-bit jobs use the race detector. Windows does not currently build because
the storage and repository locking code uses POSIX `flock`.

EUProvGuard is pinned to v1.2.1, as in go-foundation. Its exit status alone is
insufficient: any SBOM, CVE or SAST finding fails validation. Missing summaries,
unreadable reports, scanner failures and unavailable live vulnerability
databases fail too. There is no accepted-findings list. Reports are retained
even when the gate fails. Build jobs run independently of the scan, so a failed
scan can still have downloadable binaries, clearly marked as unapproved.

The CI run contains platform archives, SHA-256 files, test logs and compliance
reports as Actions artifacts, retained for 30 days. GitHub sign-in is required
for artifact downloads. A pull request receives an updated result comment with
job status, test counts, a finding severity chart and direct artifact download
links. The reporting workflow runs trusted default-branch code; PR artifacts
are parsed as data and never executed with a write-capable token.

For core, platform archives contain compiled validation test executables, not
an application. A separate source archive is included. Applications consume
the library through its Go module.

For fvs2d, when the native adapter is present, the macOS job must also compile
the FSKit app and extension and package `FVS.app` in an unsigned ZIP. Unsigned
builds are compile checks, not installable signed releases or proof of mounts.
The C archive test runs on supported 64-bit targets. Real Linux mount checks
run when the FUSE device and helper are available; otherwise the job explicitly
reports that they were not verified. Real FSKit mounts require the separately
prepared Mac and signing workflow documented by the adapter.

Pushing a `v*` tag creates or updates its GitHub Release only after validation
passes. Build archives, checksums and the compliance bundle are uploaded as
release assets. The pipeline does not create tags or publish a release for
ordinary pushes and pull requests. It does not sign or notarize macOS apps.

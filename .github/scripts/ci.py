#!/usr/bin/env python3
import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tarfile


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def compliance_result(sbom, log, exit_code=0):
    errors = []
    cve = re.findall(r"CVE findings: (\d+)", log)
    sast = re.findall(r"Native SAST completed: found (\d+) issues", log)
    findings = sbom.get("vulnerabilities", [])
    if sbom.get("bomFormat") != "CycloneDX" or not isinstance(findings, list):
        raise ValueError("invalid EUProvGuard SBOM")
    if not cve or not sast:
        errors.append("Missing CVE or SAST scan summary")
    if not isinstance(sbom.get("components"), list) or not sbom["components"]:
        errors.append("No dependency components were scanned")
    if re.search(r"\[(?:ERROR|FATAL)\]|OSV/EUVD query failed", log):
        errors.append("Live vulnerability databases could not be queried")
    if exit_code:
        errors.append(f"Scanner exited with status {exit_code}")
    cve = int(cve[-1]) if cve else None
    sast = int(sast[-1]) if sast else None
    if findings or cve or sast:
        errors.append("EUProvGuard reported findings")
    severity = collections.Counter()
    details = []
    for finding in findings:
        ratings = finding.get("ratings") or [{}]
        level = str(ratings[0].get("severity", "unknown")).lower()
        if level not in ("critical", "high", "medium", "low", "info"):
            level = "unknown"
        severity[level] += 1
        details.append({"id": str(finding.get("id", "unknown")), "severity": level,
                        "description": str(finding.get("description", ""))[:2000]})
    return {"kind": "compliance", "ok": not errors, "cve": cve, "sast": sast,
            "severity": dict(severity), "findings": details[:100], "errors": errors}


def scan(output, project):
    destination = output / "compliance"
    destination.mkdir(parents=True, exist_ok=True)
    log = destination / "scan.log"
    sbom = destination / "sbom.json"
    version = os.environ.get("GITHUB_REF_NAME", "0.0.0")
    with log.open("w") as stream:
        code = subprocess.call(["euprovguard", "-path", ".", "-name", project,
                                "-version-tag", version, "-workers", "2", "-output", str(sbom),
                                "-bundle", str(destination / "report")], stdout=stream, stderr=subprocess.STDOUT)
    text = log.read_text(errors="replace")
    print(text)
    try:
        result = compliance_result(json.loads(sbom.read_text()), text, code)
    except (OSError, ValueError, TypeError, AttributeError) as error:
        result = {"kind": "compliance", "ok": False, "errors": [str(error)]}
    write_json(output / "compliance.summary.json", result)
    print(json.dumps(result, indent=2))
    return 0 if result["ok"] else 1


def tests(output, target):
    counts = collections.Counter()
    command = ["go", "test", "-json", "-count=1", "-timeout=10m"]
    if os.environ.get("GOARCH") != "386":
        command.append("-race")
    command.append("./...")
    with (output / "tests.jsonl").open("w") as log:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            log.write(line)
            try:
                event = json.loads(line)
            except ValueError:
                print(line, end="")
                continue
            if event.get("Output"):
                print(event["Output"], end="")
            action = event.get("Action")
            if action in ("pass", "fail", "skip"):
                key = action if event.get("Test") else "package_" + action
                counts[key] += 1
        code = process.wait()
    result = {"kind": "tests", "target": target, "ok": code == 0, "counts": dict(counts)}
    write_json(output / (target + ".summary.json"), result)
    return code


def build(output, project, target):
    subprocess.run(["go", "build", "./..."], check=True)
    dist = output / "dist"
    content = output / "package"
    dist.mkdir(exist_ok=True)
    content.mkdir(exist_ok=True)
    packages = {"fvs2": ("fvs2", "git2fvs"), "fvs2d": ("fvs2d",)}
    if project == "core":
        subprocess.run(["go", "test", "-c", "-trimpath", "-o", str(content / "core.test"), "."], check=True)
        (content / "README.txt").write_text("Compiled core test executable for platform validation.\n"
                                           "Applications consume core as a Go module.\n")
        source = dist / "core-source.tar.gz"
        subprocess.run(["git", "archive", "--format=tar.gz", "-o", str(source), "HEAD"], check=True)
    else:
        for name in packages[project]:
            binary = content / name
            env = dict(os.environ, CGO_ENABLED="0")
            subprocess.run(["go", "build", "-trimpath", "-o", str(binary), "./cmd/" + name], env=env, check=True)
            argument = "-probe" if name == "fvs2d" else "--help"
            subprocess.run([str(binary), argument], check=True)
    shutil.copyfile("LICENSE", content / "LICENSE")
    archive = dist / (project + "-" + target + ".tar.gz")
    with tarfile.open(archive, "w:gz") as bundle:
        bundle.add(content, arcname=project)
    if project == "fvs2d" and platform.system() == "Darwin":
        if not Path("macos/build.sh").is_file():
            raise RuntimeError("The macOS target requires the native FSKit adapter")
        app_output = output / "native"
        subprocess.run(["bash", "macos/build.sh"], env=dict(os.environ, OUT=str(app_output)), check=True)
        subprocess.run(["ditto", "-c", "-k", "--keepParent", str(app_output / "FVS.app"),
                        str(dist / (project + "-" + target + "-unsigned.zip"))], check=True)
    for artifact in sorted(dist.iterdir()):
        if artifact.suffix in (".gz", ".zip"):
            checksum = hashlib.sha256(artifact.read_bytes()).hexdigest()
            artifact.with_name(artifact.name + ".sha256").write_text(checksum + "  " + artifact.name + "\n")


def bridge(output):
    if not Path("macos/test-bridge.sh").is_file():
        return
    fixture = output / "fixture-cli"
    revision = subprocess.check_output(["go", "list", "-m", "-f", "{{.Version}}", "github.com/fvs-lab/fvs2"], text=True).strip()
    subprocess.run(["git", "clone", "--depth=1", "--branch", revision,
                    "https://github.com/fvs-lab/fvs2.git", str(fixture)], check=True)
    cli = output / "fvs2-fixture"
    subprocess.run(["go", "build", "-trimpath", "-o", str(cli), "./cmd/fvs2"], cwd=fixture, check=True)
    subprocess.run(["bash", "macos/test-bridge.sh"],
                   env=dict(os.environ, OUT=str(output / "bridge"), FVS2=str(cli)), check=True)
    if platform.system() == "Linux":
        daemon = output / "package/fvs2d"
        probe = subprocess.check_output([str(daemon), "-probe"], text=True)
        if "mount_backend_available=true" in probe:
            subprocess.run([sys.executable, "macos/integration.py", "--daemon", str(daemon),
                            "--fvs2", str(cli), "--work-dir", str(output / "mount-tests")], check=True)
            subprocess.run(["go", "test", "./cmd/fvs2d", "-run", "TestManagerRealMountLifecycle", "-count=1"],
                           env=dict(os.environ, FVS2D_TEST_REAL_MOUNT="1"), check=True)
        else:
            print("NOT VERIFIED: real Linux mount requires /dev/fuse and fusermount")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("build", "test", "scan", "bridge"))
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--project", choices=("core", "fvs2", "fvs2d"), required=True)
    parser.add_argument("--target", default="linux-amd64")
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.action == "scan":
        return scan(args.output, args.project)
    if args.action == "test":
        return tests(args.output, args.target)
    if args.action == "bridge":
        bridge(args.output)
    else:
        build(args.output, args.project, args.target)
    return 0


if __name__ == "__main__":
    sys.exit(main())

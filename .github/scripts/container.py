"""Small GHCR publication policy; no credentials or upstream client calls in tests."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tomllib
import urllib.error
import urllib.parse
import urllib.request

import release

ROOT = Path(__file__).resolve().parents[2]
REVISION = "org.opencontainers.image.revision"
VERSION = "org.opencontainers.image.version"
ACCEPT = ", ".join(("application/vnd.oci.image.index.v1+json",
                    "application/vnd.docker.distribution.manifest.list.v2+json"))


def plan(event, name, ref, commit, root=ROOT):
    repo = event["repository"]["full_name"].lower()
    if not re.fullmatch(r"[a-z0-9_.-]+/[a-z0-9_.-]+", repo) or not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("Invalid repository or source commit")
    branch = "refs/heads/" + event["repository"]["default_branch"]
    version = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))["package"]["version"]
    tag, channel, publish = "dev-" + commit, "dev", name != "pull_request"
    if name == "release":
        published = event["release"]
        if event.get("action") != "published" or published["draft"]:
            raise ValueError("Only an explicitly published Release may publish a version image")
        info, _ = release.metadata(root, published["tag_name"])
        version = info["version"]
        tag = version.replace("+", "_")
        channel = "" if info["prerelease"] or published["prerelease"] else "latest"
    elif name not in ("push", "workflow_dispatch", "pull_request") or (publish and ref != branch):
        raise ValueError("Development images must come from the default branch")
    if len(tag) > 128:
        raise ValueError("Version is too long for a container tag")
    return dict(image="ghcr.io/" + repo, commit=commit, version=version,
                tag=tag, channel=channel, publish=publish, branch=branch)


class Registry:
    def __init__(self, image):
        if not re.fullmatch(r"ghcr.io/[a-z0-9_.-]+/[a-z0-9_.-]+", image):
            raise ValueError("Expected a GHCR repository")
        self.repository = image.removeprefix("ghcr.io/")
        self.token = None

    def manifest(self, reference):
        if not re.fullmatch(r"[a-zA-Z0-9_.:-]+", reference):
            raise ValueError("Invalid manifest reference")
        if self.token is None:
            query = urllib.parse.urlencode(dict(service="ghcr.io", scope=f"repository:{self.repository}:pull"))
            auth = base64.b64encode((os.environ["GITHUB_ACTOR"] + ":" + os.environ["GITHUB_TOKEN"]).encode()).decode()
            request = urllib.request.Request("https://ghcr.io/token?" + query,
                                             headers={"Authorization": "Basic " + auth})
            with urllib.request.urlopen(request, timeout=30) as response:
                self.token = json.load(response)["token"]
        request = urllib.request.Request(f"https://ghcr.io/v2/{self.repository}/manifests/{reference}",
                                         headers={"Authorization": "Bearer " + self.token, "Accept": ACCEPT})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read(4 * 1024 * 1024 + 1)
                digest = response.headers.get("Docker-Content-Digest", "")
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
            raise RuntimeError(f"Registry lookup failed (HTTP {error.code}); refusing to assume a missing tag") from None
        if len(raw) > 4 * 1024 * 1024 or digest != "sha256:" + hashlib.sha256(raw).hexdigest():
            raise ValueError("Invalid registry manifest size or digest")
        manifest = json.loads(raw)
        manifest["digest"] = digest
        return manifest


def existing_digest(manifest, info):
    if manifest is None:
        return ""
    annotations = manifest.get("annotations", {})
    platforms = {m.get("platform", {}).get("architecture") for m in manifest.get("manifests", [])
                 if m.get("platform", {}).get("os") == "linux"}
    if annotations.get(REVISION) != info["commit"] or annotations.get(VERSION) != info["version"] or platforms != {"amd64", "arm64"}:
        raise ValueError("Immutable image tag exists with different or unverified contents; refusing overwrite")
    return manifest["digest"]


def newer_stable(version, current):
    candidate = release.VERSION.fullmatch(version)
    if candidate is None or candidate[4]:
        return False
    if current is None:
        return True
    previous = release.VERSION.fullmatch(current.get("annotations", {}).get(VERSION, ""))
    if previous is None or previous[4]:
        raise ValueError("Existing latest has no stable version evidence; refusing automatic replacement")
    return tuple(map(int, candidate.group(1, 2, 3))) > tuple(map(int, previous.group(1, 2, 3)))


def smoke(descriptor, info, arch):
    identity = descriptor["identity"]
    target = {"amd64": "x86_64-unknown-linux-gnu", "arm64": "aarch64-unknown-linux-gnu"}[arch]
    if (descriptor["schemaVersion"] != 1 or descriptor["peerKind"] != "rust_native"
            or descriptor["launchSupported"] is not True or identity["buildCommit"] != info["commit"]
            or identity["implementationVersion"] != info["version"] or identity["target"] != target
            or not re.fullmatch(r"[0-9a-f]{64}", identity["artifactSha256"])):
        raise ValueError("Container offline descriptor does not match source/platform")


def output(**values):
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as stream:
        for key, value in values.items():
            stream.write(f"{key}={str(value).lower() if isinstance(value, bool) else value}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("plan", "publish", "smoke"))
    parser.add_argument("--descriptor", type=Path)
    parser.add_argument("--arch", choices=("amd64", "arm64"))
    args = parser.parse_args()
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text(encoding="utf-8"))
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    info = plan(event, os.environ["GITHUB_EVENT_NAME"], os.environ["GITHUB_REF"], commit)
    if args.command == "smoke":
        smoke(json.loads(args.descriptor.read_text(encoding="utf-8")), info, args.arch)
        return
    registry = Registry(info["image"])
    existing = existing_digest(registry.manifest(info["tag"]), info) if info["publish"] else ""
    if args.command == "plan":
        output(**info, existing=existing)
        return
    if not info["publish"]:
        raise ValueError("Pull requests cannot publish")
    if not existing:
        digests = [p.name for p in Path("digests").iterdir()]
        if len(digests) != 2 or any(not re.fullmatch(r"[0-9a-f]{64}", d) for d in digests):
            raise ValueError("Expected exactly two verified platform digests")
        subprocess.run(["docker", "buildx", "imagetools", "create", "--tag", info["image"] + ":" + info["tag"],
                        "--annotation", "index:" + REVISION + "=" + commit,
                        "--annotation", "index:" + VERSION + "=" + info["version"],
                        *[info["image"] + "@sha256:" + d for d in sorted(digests)]], check=True)
        existing = existing_digest(registry.manifest(info["tag"]), info)
        if not existing:
            raise ValueError("Published immutable manifest is not readable")
    promote = False
    if info["channel"] == "latest":
        promote = newer_stable(info["version"], registry.manifest("latest"))
    elif info["channel"] == "dev":
        head = subprocess.check_output(["git", "ls-remote", "--exit-code", "origin", info["branch"]], text=True).split()[0]
        promote = head == commit
    if promote:
        subprocess.run(["docker", "buildx", "imagetools", "create", "--tag", info["image"] + ":" + info["channel"],
                        info["image"] + "@" + existing], check=True)
    print(f"Published/reused {info['image']}:{info['tag']} ({existing}); rolling tag updated: {promote}")


if __name__ == "__main__":
    main()

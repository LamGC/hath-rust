"""Prepare immutable release assets; the only executed binary is an offline probe."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[2]
PRIMARY_FINGERPRINT = "7EF5FB1E65C1A6C6D02DFCDFCE57746D7E5EE6EF"
TARGETS = (
    "x86_64-unknown-linux-gnu", "x86_64-unknown-linux-musl",
    "aarch64-unknown-linux-gnu", "aarch64-unknown-linux-musl",
    "armv7-unknown-linux-gnueabihf", "armv7-unknown-linux-musleabihf",
    "x86_64-pc-windows-msvc", "i686-pc-windows-msvc", "aarch64-pc-windows-msvc",
    "universal-apple-darwin", "aarch64-linux-android", "armv7-linux-androideabi",
    "i686-linux-android", "x86_64-linux-android",
)
VERSION = re.compile(
    r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
)


def metadata(root, tag):
    version = tag.removeprefix("v")
    match = VERSION.fullmatch(version)
    if not tag.startswith("v") or not match:
        raise ValueError("Expected a v-prefixed SemVer tag")
    if any(len(x) > 1 and x.startswith("0") and x.isdecimal() for x in (match[4] or "").split(".")):
        raise ValueError("Prerelease numbers must not have leading zeroes")
    cargo = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    lock = tomllib.loads((root / "Cargo.lock").read_text(encoding="utf-8"))
    packages = [p for p in lock["package"] if p["name"] == "hath-rust"]
    if cargo["package"]["name"] != "hath-rust" or cargo["package"]["version"] != version or len(packages) != 1 or packages[0]["version"] != version:
        raise ValueError("Tag, Cargo.toml and Cargo.lock must agree")
    if "manager-integration" not in cargo["features"]["default"]:
        raise ValueError("Release must include Manager integration")
    changelog = (root / "CHANGELOG.md").read_text(encoding="utf-8")
    sections = list(re.finditer(r"^## \[([^\]\r\n]+)\][^\r\n]*$", changelog, re.MULTILINE))
    matches = [i for i, section in enumerate(sections) if section[1] == version]
    if len(matches) != 1:
        raise ValueError("Expected exactly one changelog section for this version")
    index = matches[0]
    end = sections[index + 1].start() if index + 1 < len(sections) else len(changelog)
    notes = changelog[sections[index].start():end].strip() + "\n"
    if len(notes.splitlines()) < 3:
        raise ValueError("Release notes are empty")
    return {"version": version, "tag": tag, "prerelease": bool(match[4])}, notes


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def verify_tag(status):
    forbidden = ("BADSIG", "ERRSIG", "EXPSIG", "EXPKEYSIG", "REVKEYSIG", "FAILURE")
    if any(line.startswith("[GNUPG:] " + name + " ") for name in forbidden for line in status.splitlines()):
        raise ValueError("Tag signature is invalid, expired or revoked")
    signatures = [line.split() for line in status.splitlines() if line.startswith("[GNUPG:] VALIDSIG ")]
    if len(signatures) != 1 or len(signatures[0]) < 11 or PRIMARY_FINGERPRINT not in (signatures[0][2], signatures[0][-1]):
        raise ValueError("Tag must have a valid signature from the pinned primary key")


def validate_binary(path, target):
    if path.is_symlink() or path.parent.is_symlink() or not path.is_file():
        raise ValueError("Missing or symlinked artifact: " + target)
    if not 20 <= path.stat().st_size <= 256 * 1024 * 1024:
        raise ValueError("Invalid binary size: " + target)
    with path.open("rb") as stream:
        header = stream.read(20)
    if "windows" in target:
        valid = header[:2] == b"MZ"
    elif "darwin" in target:
        valid = header[:4] in (b"\xca\xfe\xba\xbe", b"\xca\xfe\xba\xbf")
    else:
        machine = 183 if target.startswith("aarch64") else 62 if target.startswith("x86_64") else 40 if target.startswith("armv7") else 3
        bits = 2 if machine in (183, 62) else 1
        valid = header[:6] == b"\x7fELF" + bytes((bits, 1)) and int.from_bytes(header[18:20], "little") == machine
    if not valid:
        raise ValueError("Binary architecture mismatch: " + target)


def probe(path, version, commit):
    with tempfile.TemporaryDirectory(prefix="hath-release-probe-") as work:
        result = subprocess.run([str(path.resolve()), "--manager-describe", "--json"],
                                cwd=work, stdin=subprocess.DEVNULL,
                                capture_output=True, check=True, timeout=30)
        if list(Path(work).iterdir()) or result.stderr or not 0 < len(result.stdout) <= 65536:
            raise ValueError("Offline descriptor must be bounded and side-effect free")
    descriptor = json.loads(result.stdout)
    identity = descriptor["identity"]
    if descriptor["schemaVersion"] != 1 or descriptor["peerKind"] != "rust_native" or descriptor["launchSupported"] is not True or identity["implementationVersion"] != version or identity["buildCommit"] != commit or identity["artifactSha256"] != digest(path) or identity["target"] != "x86_64-unknown-linux-musl":
        raise ValueError("Offline descriptor identity mismatch")
    return descriptor


def prepare(root, tag, artifacts, destination, notes_path):
    info, notes = metadata(root, tag)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("Unknown source commit")
    if destination.exists():
        raise ValueError("Release output directory must be new")
    sources = []
    for target in TARGETS:
        suffix = ".exe" if "windows" in target else ""
        source = artifacts / ("hath-rust-" + target) / ("hath-rust" + suffix)
        validate_binary(source, target)
        sources.append((target, suffix, source))
    destination.mkdir(parents=True)
    assets = []
    for target, suffix, source in sources:
        name = "hath-rust-" + target + suffix
        binary = destination / name
        shutil.copyfile(source, binary)
        binary.chmod(0o755)
        assets.append({"fileName": name, "target": target, "sizeBytes": binary.stat().st_size,
                       "sha256": digest(binary), "downloadUrl": f"https://github.com/LamGC/hath-rust/releases/download/{tag}/{name}"})
    descriptor = probe(destination / "hath-rust-x86_64-unknown-linux-musl", info["version"], commit)
    manifest = {"schemaVersion": 1, "repository": "LamGC/hath-rust", **info, "sourceCommit": commit,
                "managerProtocol": descriptor["protocol"], "integrationVersion": descriptor["identity"]["integrationVersion"],
                "assets": assets}
    (destination / "release-manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    files = sorted(destination.iterdir())
    (destination / "SHA256SUMS").write_text("".join(digest(path) + "  " + path.name + "\n" for path in files), encoding="ascii")
    notes_path.write_text(notes, encoding="utf-8")
    return manifest


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check")
    check.add_argument("--tag", required=True)
    verify = commands.add_parser("verify-tag")
    verify.add_argument("--status", required=True, type=Path)
    package = commands.add_parser("prepare")
    package.add_argument("--tag", required=True)
    package.add_argument("--artifacts", required=True, type=Path)
    package.add_argument("--destination", required=True, type=Path)
    package.add_argument("--notes", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "check":
        info = metadata(ROOT, args.tag)[0]
        print(json.dumps(info))
        if os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as stream:
                stream.write("prerelease=" + str(info["prerelease"]).lower() + "\n")
    elif args.command == "verify-tag":
        verify_tag(args.status.read_text(encoding="utf-8"))
    else:
        prepare(ROOT, args.tag, args.artifacts, args.destination, args.notes)


if __name__ == "__main__":
    main()

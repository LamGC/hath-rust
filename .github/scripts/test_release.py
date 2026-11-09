import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("release", Path(__file__).with_name("release.py"))
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cargo = '[package]\nname="hath-rust"\nversion="1.19.0"\n[features]\ndefault=["manager-integration"]\n'
        self.lock = '[[package]]\nname="hath-rust"\nversion="1.19.0"\n'
        self.notes = '# 更新日志\n\n## [1.19.0] - 2026-11-09\n\n### 本项目改动\n\n- 测试。\n'
        self.write_metadata()

    def write_metadata(self):
        for name, value in (("Cargo.toml", self.cargo), ("Cargo.lock", self.lock), ("CHANGELOG.md", self.notes)):
            (self.root / name).write_text(value, encoding="utf-8")

    def test_version_and_unicode_notes(self):
        info, notes = release.metadata(self.root, "v1.19.0")
        self.assertEqual(info, {"version": "1.19.0", "tag": "v1.19.0", "prerelease": False})
        self.assertIn("本项目改动", notes)

    def test_invalid_tags(self):
        for tag in ("1.19.0", "v01.19.0", "v1.19", "v1.19.0\n", "v1.19.0-01", "v1.19.0/evil", "v1.19.0-", "v1.19.0+", "vv1.19.0"):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                release.metadata(self.root, tag)

    def test_prerelease_and_build_metadata(self):
        for version, expected in (("1.19.0-rc.1", True), ("1.19.0+build-1", False)):
            self.cargo = self.cargo.replace("1.19.0", version)
            self.lock = self.lock.replace("1.19.0", version)
            self.notes = self.notes.replace("1.19.0", version)
            self.write_metadata()
            self.assertEqual(release.metadata(self.root, "v" + version)[0]["prerelease"], expected)
            self.cargo = self.cargo.replace(version, "1.19.0")
            self.lock = self.lock.replace(version, "1.19.0")
            self.notes = self.notes.replace(version, "1.19.0")

    def test_cargo_mismatch(self):
        self.cargo = self.cargo.replace("1.19.0", "1.20.0")
        self.write_metadata()
        with self.assertRaises(ValueError):
            release.metadata(self.root, "v1.19.0")

    def test_lock_mismatch_or_duplicate(self):
        for value in (self.lock.replace("1.19.0", "1.20.0"), self.lock * 2):
            (self.root / "Cargo.lock").write_text(value, encoding="utf-8")
            with self.assertRaises(ValueError):
                release.metadata(self.root, "v1.19.0")

    def test_integration_must_be_enabled(self):
        self.cargo = self.cargo.replace('["manager-integration"]', '[]')
        self.write_metadata()
        with self.assertRaises(ValueError):
            release.metadata(self.root, "v1.19.0")

    def test_missing_duplicate_and_empty_notes(self):
        for notes in ("# 更新日志\n", self.notes + "\n## [1.19.0]\n\n- duplicate\n", "## [1.19.0]\n"):
            (self.root / "CHANGELOG.md").write_text(notes, encoding="utf-8")
            with self.assertRaises(ValueError):
                release.metadata(self.root, "v1.19.0")

    def test_only_selected_changelog_section(self):
        self.notes = self.notes + '\n## [1.18.0]\n\n- Not this release.\n'
        self.write_metadata()
        self.assertNotIn("Not this release", release.metadata(self.root, "v1.19.0")[1])

    def make_assets(self):
        artifacts = self.root / "artifacts"
        for target in release.TARGETS:
            directory = artifacts / ("hath-rust-" + target)
            directory.mkdir(parents=True)
            header = bytearray(128)
            if "windows" in target:
                header[:2] = b"MZ"
            elif "darwin" in target:
                header[:4] = b"\xca\xfe\xba\xbe"
            else:
                machine = 183 if target.startswith("aarch64") else 62 if target.startswith("x86_64") else 40 if target.startswith("armv7") else 3
                header[:6] = b"\x7fELF" + bytes((2 if machine in (183, 62) else 1, 1))
                header[18:20] = machine.to_bytes(2, "little")
            (directory / ("hath-rust.exe" if "windows" in target else "hath-rust")).write_bytes(header)
        return artifacts

    def descriptor(self):
        return {"schemaVersion": 1, "peerKind": "rust_native", "launchSupported": True,
                "protocol": {"major": 1, "minor": 0},
                "identity": {"implementationVersion": "1.19.0", "buildCommit": "a" * 40,
                             "artifactSha256": "unused", "target": "x86_64-unknown-linux-musl", "integrationVersion": "1.0.0"}}

    def prepare(self, artifacts):
        with mock.patch.object(release.subprocess, "check_output", return_value="a" * 40 + "\n"), mock.patch.object(release, "probe", return_value=self.descriptor()):
            return release.prepare(self.root, "v1.19.0", artifacts, self.root / "dist", self.root / "notes.md")

    def test_prepare_all_assets_and_manifest_checksums(self):
        manifest = self.prepare(self.make_assets())
        self.assertEqual(len(manifest["assets"]), 14)
        dist = self.root / "dist"
        for line in (dist / "SHA256SUMS").read_text().splitlines():
            sha, name = line.split("  ", 1)
            self.assertEqual(sha, release.digest(dist / name))
        self.assertEqual(len((dist / "SHA256SUMS").read_text().splitlines()), 15)
        self.assertEqual(json.loads((dist / "release-manifest.json").read_text())["sourceCommit"], "a" * 40)
        self.assertIn("本项目改动", (self.root / "notes.md").read_text())

    def test_missing_asset_rejected_before_any_output(self):
        artifacts = self.make_assets()
        (artifacts / "hath-rust-i686-linux-android/hath-rust").unlink()
        with self.assertRaises(ValueError):
            self.prepare(artifacts)
        self.assertFalse((self.root / "dist").exists())

    def test_symlink_asset_rejected(self):
        artifacts = self.make_assets()
        binary = artifacts / "hath-rust-x86_64-unknown-linux-musl/hath-rust"
        saved = self.root / "saved"
        binary.rename(saved)
        binary.symlink_to(saved)
        with self.assertRaises(ValueError):
            self.prepare(artifacts)

    def test_output_directory_cannot_be_reused(self):
        artifacts = self.make_assets()
        self.prepare(artifacts)
        with self.assertRaises(ValueError):
            self.prepare(artifacts)

    def test_wrong_architecture_and_empty_binary_rejected(self):
        binary = self.root / "binary"
        for content in (b"", bytes(128)):
            binary.write_bytes(content)
            with self.assertRaises(ValueError):
                release.validate_binary(binary, "x86_64-unknown-linux-musl")
        header = bytearray(128)
        header[:6] = b"\x7fELF\x02\x01"
        header[18:20] = (183).to_bytes(2, "little")
        binary.write_bytes(header)
        with self.assertRaises(ValueError):
            release.validate_binary(binary, "x86_64-unknown-linux-musl")

    def test_probe_rejects_identity_mismatch(self):
        binary = self.root / "binary"
        binary.write_bytes(b"not executed")
        descriptor = self.descriptor()
        result = mock.Mock(stdout=json.dumps(descriptor).encode(), stderr=b"")
        with mock.patch.object(release.subprocess, "run", return_value=result), self.assertRaises(ValueError):
            release.probe(binary, "1.19.0", "a" * 40)

    def test_tag_signature_accepts_signing_subkey_under_pinned_primary(self):
        status = "[GNUPG:] VALIDSIG " + "A" * 40 + " 2026-11-09 1794207207 0 4 0 1 10 00 " + release.PRIMARY_FINGERPRINT
        release.verify_tag(status)

    def test_tag_signature_accepts_pinned_primary_itself(self):
        status = "[GNUPG:] VALIDSIG " + release.PRIMARY_FINGERPRINT + " 2026-11-09 1794207207 0 4 0 1 10 00"
        release.verify_tag(status)

    def test_tag_signature_rejects_other_primary_missing_or_ambiguous_signature(self):
        status = "[GNUPG:] VALIDSIG " + "A" * 40 + " 2026-11-09 1794207207 0 4 0 1 10 00 " + "B" * 40
        for value in (status, "[GNUPG:] GOODSIG key name", "", status + "\n" + status):
            with self.subTest(status=value), self.assertRaises(ValueError):
                release.verify_tag(value)

    def test_tag_signature_rejects_revocation_even_with_validsig(self):
        status = "[GNUPG:] VALIDSIG " + "A" * 40 + " 2026-11-09 1794207207 0 4 0 1 10 00 " + release.PRIMARY_FINGERPRINT
        for name in ("BADSIG", "ERRSIG", "EXPSIG", "EXPKEYSIG", "REVKEYSIG", "FAILURE"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                release.verify_tag(status + "\n[GNUPG:] " + name + " invalid")


if __name__ == "__main__":
    unittest.main()

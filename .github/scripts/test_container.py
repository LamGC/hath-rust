import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import urllib.error

SPEC = importlib.util.spec_from_file_location("container_policy", Path(__file__).with_name("container.py"))
policy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(policy)


class ContainerTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        (self.root / "Cargo.toml").write_text('[package]\nname="hath-rust"\nversion="1.19.0"\n[features]\ndefault=["manager-integration"]\n')
        (self.root / "Cargo.lock").write_text('[[package]]\nname="hath-rust"\nversion="1.19.0"\n')
        (self.root / "CHANGELOG.md").write_text('## [1.19.0]\n\n- Test release.\n')
        self.event = {"repository": {"full_name": "LamGC/hath-rust", "default_branch": "main"},
                      "action": "published", "release": {"draft": False, "prerelease": False, "tag_name": "v1.19.0"}}
        self.commit = "a" * 40
        self.info = policy.plan(self.event, "push", "refs/heads/main", self.commit, self.root)

    def test_development_is_not_formal_and_pr_cannot_publish(self):
        self.assertEqual(self.info["tag"], "dev-" + self.commit)
        self.assertEqual(self.info["channel"], "dev")
        self.assertEqual(self.info["image"], "ghcr.io/lamgc/hath-rust")
        self.assertFalse(policy.plan(self.event, "pull_request", "refs/pull/5/merge", self.commit, self.root)["publish"])
        for name in ("push", "workflow_dispatch", "pull_request_target"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                policy.plan(self.event, name, "refs/heads/topic", self.commit, self.root)

    def test_only_published_release_produces_version(self):
        info = policy.plan(self.event, "release", "refs/tags/v1.19.0", self.commit, self.root)
        self.assertEqual((info["tag"], info["channel"]), ("1.19.0", "latest"))
        self.event["release"]["prerelease"] = True
        self.assertEqual(policy.plan(self.event, "release", "", self.commit, self.root)["channel"], "")
        for change in (dict(action="created"), dict(release={**self.event["release"], "draft": True})):
            with self.assertRaises(ValueError):
                policy.plan({**self.event, **change}, "release", "", self.commit, self.root)

    def test_semver_prerelease_and_metadata(self):
        for path in self.root.iterdir():
            path.write_text(path.read_text().replace("1.19.0", "1.20.0-rc.1+build.1"))
        self.event["release"]["tag_name"] = "v1.20.0-rc.1+build.1"
        info = policy.plan(self.event, "release", "", self.commit, self.root)
        self.assertEqual((info["tag"], info["channel"]), ("1.20.0-rc.1_build.1", ""))

    def manifest(self, version="1.19.0"):
        return {"digest": "sha256:" + "b" * 64,
                "annotations": {policy.REVISION: self.commit, policy.VERSION: version},
                "manifests": [{"platform": {"os": "linux", "architecture": arch}} for arch in ("amd64", "arm64")]}

    def test_existing_immutable_is_reused_only_with_source_and_platform_evidence(self):
        manifest = self.manifest()
        self.assertEqual(policy.existing_digest(None, self.info), "")
        self.assertEqual(policy.existing_digest(manifest, self.info), manifest["digest"])
        for mutation in (dict(annotations={}), dict(manifests=[]), dict(annotations={policy.REVISION: "c" * 40, policy.VERSION: "1.19.0"})):
            with self.assertRaises(ValueError):
                policy.existing_digest({**manifest, **mutation}, self.info)

    def test_latest_never_rolls_back_or_promotes_prerelease(self):
        self.assertTrue(policy.newer_stable("1.19.0", None))
        self.assertTrue(policy.newer_stable("1.20.0", self.manifest()))
        for version in ("1.19.0", "1.18.99", "1.19.0+rebuild", "2.0.0-rc.1"):
            self.assertFalse(policy.newer_stable(version, self.manifest()))
        with self.assertRaises(ValueError):
            policy.newer_stable("1.20.0", {"annotations": {}})

    def test_registry_only_404_is_missing_not_auth_or_network_failure(self):
        registry = policy.Registry("ghcr.io/lamgc/hath-rust")
        registry.token = "test-never-sent"
        for code in (401, 403, 429, 500):
            with mock.patch.object(policy.urllib.request, "urlopen", side_effect=urllib.error.HTTPError("", code, "", {}, None)), self.assertRaises(RuntimeError):
                registry.manifest("dev")
        with mock.patch.object(policy.urllib.request, "urlopen", side_effect=urllib.error.HTTPError("", 404, "", {}, None)):
            self.assertIsNone(registry.manifest("dev"))

    def test_registry_checks_content_digest(self):
        registry = policy.Registry("ghcr.io/lamgc/hath-rust")
        registry.token = "test"
        raw = json.dumps(self.manifest()).encode()
        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.read.return_value = raw
        response.headers = {"Docker-Content-Digest": "sha256:" + hashlib.sha256(raw).hexdigest()}
        with mock.patch.object(policy.urllib.request, "urlopen", return_value=response):
            self.assertEqual(registry.manifest("dev")["digest"], response.headers["Docker-Content-Digest"])
            response.headers = {}
            with self.assertRaises(ValueError):
                registry.manifest("dev")

    def test_smoke_verifies_binary_source_version_and_platform(self):
        descriptor = {"schemaVersion": 1, "peerKind": "rust_native", "launchSupported": True,
                      "identity": {"buildCommit": self.commit, "implementationVersion": "1.19.0",
                                   "target": "aarch64-unknown-linux-gnu", "artifactSha256": "b" * 64}}
        policy.smoke(descriptor, self.info, "arm64")
        for field, value in (("buildCommit", "unknown"), ("implementationVersion", "1.18.0"), ("artifactSha256", "bad")):
            changed = copy.deepcopy(descriptor)
            changed["identity"][field] = value
            with self.assertRaises(ValueError):
                policy.smoke(changed, self.info, "arm64")
        with self.assertRaises(ValueError):
            policy.smoke(descriptor, self.info, "amd64")


if __name__ == "__main__":
    unittest.main()

//! External deployment identity and append-only boot registry. The directory lock
//! lives for the client lifetime; none of these files contain the access credential.
use super::{
    config::{Config, ExternalConnection, canonical_id, invalid, open_no_follow, read_private, safe_segment},
    protocol::{self, ExternalIdentity, NativeIdentity, Registration},
};
use rustix::fs::{Mode, OFlags, RenameFlags, mkdirat, openat, renameat_with};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{self, Read, Write},
    os::{fd::AsRawFd, unix::fs::MetadataExt},
    path::{Component, Path, PathBuf},
};

const MAX_BOOTS: usize = 1024;
const MAX_DOCUMENT: usize = 64 * 1024;

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Settings {
    pub mode: ExternalMode,
    pub identity_id: String,
    pub rpc_url: String,
    pub credential_file: PathBuf,
    pub spool_directory: PathBuf,
    pub download_root: PathBuf,
    #[serde(default)]
    pub allow_private_http: bool,
    pub ca_certificate_file: Option<PathBuf>,
    #[serde(default = "heartbeat_default")]
    pub heartbeat_millis: u64,
    #[serde(default = "snapshot_default")]
    pub snapshot_millis: u64,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum ExternalMode {
    External,
}

fn heartbeat_default() -> u64 {
    30_000
}
fn snapshot_default() -> u64 {
    10_000
}

impl Settings {
    pub fn read(path: &Path) -> io::Result<Self> {
        let value: Self = protocol::decode(&read_private(path, MAX_DOCUMENT as u64)?).map_err(invalid)?;
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> io::Result<()> {
        if !safe_segment(&self.identity_id)
            || !canonical_path(&self.spool_directory)
            || !canonical_path(&self.download_root)
            || self.spool_directory.starts_with(&self.download_root)
            || self.download_root.starts_with(&self.spool_directory)
        {
            return Err(invalid("manager_external_configuration"));
        }
        self.config("11111111-1111-4111-8111-111111111111", 1).validate()
    }

    fn config(&self, instance_id: &str, generation: u64) -> Config {
        Config {
            external: Some(ExternalConnection {
                identity: ExternalIdentity {
                    identity_id: self.identity_id.clone(),
                    run_generation: generation,
                    purpose: "live".into(),
                },
                allow_private_http: self.allow_private_http,
                ca_certificate_file: self.ca_certificate_file.clone(),
            }),
            instance_id: instance_id.into(),
            stable_instance_key: self.identity_id.clone(),
            rpc_url: self.rpc_url.clone(),
            credential_file: self.credential_file.clone(),
            spool_directory: self.spool_directory.join("outbox"),
            heartbeat_millis: self.heartbeat_millis,
            snapshot_millis: self.snapshot_millis,
        }
    }

    fn binding(&self) -> io::Result<Binding> {
        Ok(Binding {
            schema_version: 1,
            identity_id: self.identity_id.clone(),
            rpc_url: self.config("11111111-1111-4111-8111-111111111111", 1).endpoint()?.to_string(),
            download_root: self.download_root.clone(),
        })
    }
}

fn canonical_path(path: &Path) -> bool {
    path.is_absolute()
        && path != Path::new("/")
        && path
            .components()
            .all(|part| matches!(part, Component::RootDir | Component::Normal(_)))
        && path
            .to_str()
            .is_some_and(|raw| !raw.contains("//") && !raw.contains("/./") && !raw.ends_with("/.") && !raw.ends_with('/'))
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Binding {
    schema_version: u8,
    identity_id: String,
    rpc_url: String,
    download_root: PathBuf,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DeploymentState {
    binding: Binding,
    allocated_generation: u64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Boot {
    pub schema_version: u8,
    pub instance_id: String,
    pub generation: u64,
    // Preserve the original registration, including the native binary identity
    // and advertised capabilities, across upgrades and replay.
    pub registration: serde_json::Value,
}

impl Boot {
    fn name(&self) -> String {
        format!("{:016}-{}.json", self.generation, self.instance_id)
    }

    pub(super) fn validate(&self, identity: &str) -> io::Result<()> {
        if self.schema_version != 1
            || !canonical_id(&self.instance_id)
            || self.registration["instanceId"].as_str() != Some(&self.instance_id)
            || self.registration["peerKind"].as_str() != Some(protocol::PEER_KIND)
        {
            return Err(invalid("manager_external_boot_invalid"));
        }
        let external: ExternalIdentity =
            serde_json::from_value(self.registration["externalIdentity"].clone()).map_err(|_| invalid("manager_external_boot_invalid"))?;
        external.validate().map_err(invalid)?;
        if external.identity_id != identity || external.run_generation != self.generation || external.purpose != "live" {
            return Err(invalid("manager_external_boot_identity"));
        }
        if self.registration["protocol"]["major"] != 1
            || self.registration["protocol"]["minor"] != 1
            || self.registration["protocol"]["outputProfile"] != protocol::PROFILE
            || !self.registration["protocol"]["requiredCapabilities"]
                .as_array()
                .is_some_and(|values| values.iter().any(|name| name == protocol::EXTERNAL_CAPABILITY))
        {
            return Err(invalid("manager_external_boot_protocol"));
        }
        Ok(())
    }

    pub fn replay_registration(&self) -> serde_json::Value {
        let mut registration = self.registration.clone();
        registration["externalIdentity"]["purpose"] = "replay".into();
        registration
    }
}

pub(super) struct Registry {
    pub config: Config,
    pub current: Boot,
    pub history: Vec<Boot>,
    // Keep this lock until sender/persistence workers have joined.
    _lock: std::sync::Arc<File>,
}

#[derive(Clone)]
pub(super) struct RecoverySource {
    pub download_root: PathBuf,
    // A cancelled hash worker still owns the fence until it stops touching files.
    pub fence: std::sync::Arc<File>,
}

impl Registry {
    pub fn recovery_source(&self, settings: &Settings) -> RecoverySource {
        RecoverySource {
            download_root: settings.download_root.clone(),
            fence: self._lock.clone(),
        }
    }
    pub fn start(settings: &Settings, identity: &NativeIdentity) -> io::Result<Self> {
        Self::start_locked(settings, identity, Self::lock(settings)?)
    }

    pub fn lock(settings: &Settings) -> io::Result<std::sync::Arc<File>> {
        settings.validate()?;
        let root = open_no_follow(&settings.spool_directory)?;
        validate_directory(&root)?;
        rustix::fs::flock(&root, rustix::fs::FlockOperation::NonBlockingLockExclusive)
            .map_err(|_| invalid("manager_external_profile_busy"))?;
        Ok(std::sync::Arc::new(root))
    }

    pub fn start_locked(settings: &Settings, identity: &NativeIdentity, lock: std::sync::Arc<File>) -> io::Result<Self> {
        let root = lock.as_ref();
        let binding = settings.binding()?;
        let existing = read_optional(root, "binding.json")?;
        let mut state = if let Some(bytes) = &existing {
            let previous: DeploymentState = protocol::decode(bytes).map_err(invalid)?;
            if previous.binding != binding {
                return Err(invalid("manager_external_binding_changed"));
            }
            previous
        } else {
            // Never silently rebind a preexisting managed spool or a registry
            // whose binding was removed. Only our unpublished stage may remain.
            for entry in std::fs::read_dir(fd_path(root))? {
                if entry?.file_name() != ".stage" {
                    return Err(invalid("manager_external_binding_missing"));
                }
            }
            let initial = DeploymentState {
                binding,
                allocated_generation: 0,
            };
            write_document(root, "binding.json", &serde_json::to_vec(&initial).map_err(|_| invalid("manager_external_encoding"))?, false)?;
            initial
        };
        let boots = child_directory(root, "boots")?;
        let outbox = child_directory(root, "outbox")?;
        root.sync_all()?;
        let mut history = Vec::new();
        let mut history_bytes = 0;
        for entry in std::fs::read_dir(fd_path(&boots))? {
            let name = entry?
                .file_name()
                .into_string()
                .map_err(|_| invalid("manager_external_boot_name"))?;
            if name == ".stage" {
                continue;
            }
            if history.len() >= MAX_BOOTS {
                return Err(invalid("manager_external_boot_capacity"));
            }
            let bytes = read_optional(&boots, &name)?.ok_or_else(|| invalid("manager_external_boot_missing"))?;
            history_bytes += bytes.len();
            if history_bytes > 4 * 1024 * 1024 {
                return Err(invalid("manager_external_boot_capacity"));
            }
            let boot: Boot = protocol::decode(&bytes).map_err(invalid)?;
            boot.validate(&settings.identity_id)?;
            if name != boot.name() || boot.generation > state.allocated_generation {
                return Err(invalid("manager_external_boot_name"));
            }
            history.push(boot);
        }
        history.sort_by_key(|boot| boot.generation);
        if history.windows(2).any(|pair| pair[0].generation == pair[1].generation) || history.len() >= MAX_BOOTS {
            return Err(invalid("manager_external_boot_capacity"));
        }
        // A run with persisted events but no original registration cannot safely
        // be replayed. Do not invent a new identity or quietly strand its events.
        for (index, entry) in std::fs::read_dir(fd_path(&outbox))?.enumerate() {
            let name = entry?
                .file_name()
                .into_string()
                .map_err(|_| invalid("manager_external_boot_name"))?;
            if index > MAX_BOOTS || name != "quarantine" && !history.iter().any(|boot| boot.instance_id == name) {
                return Err(invalid("manager_external_boot_missing"));
            }
        }
        let generation = state
            .allocated_generation
            .checked_add(1)
            .ok_or_else(|| invalid("manager_external_generation_exhausted"))?;
        let instance_id = super::new_id()?;
        let config = settings.config(&instance_id, generation);
        config.validate()?;
        let external = &config.external.as_ref().unwrap().identity;
        let current = Boot {
            schema_version: 1,
            instance_id,
            generation,
            registration: serde_json::to_value(Registration::external(&config.instance_id, identity, external))
                .map_err(|_| invalid("manager_external_encoding"))?,
        };
        // Allocate and fsync before publishing the immutable boot. An interrupted
        // startup may leave a gap, but even a missing boot file cannot reuse its
        // generation. Missing snapshots will never be rebuilt from new identity.
        state.allocated_generation = generation;
        write_document(root, "binding.json", &serde_json::to_vec(&state).map_err(|_| invalid("manager_external_encoding"))?, true)?;
        write_document(&boots, &current.name(), &serde_json::to_vec(&current).map_err(|_| invalid("manager_external_encoding"))?, false)?;
        Ok(Self {
            config,
            current,
            history,
            _lock: lock,
        })
    }
}

fn fd_path(file: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
}
fn validate_directory(file: &File) -> io::Result<()> {
    let meta = file.metadata()?;
    if !meta.is_dir() || meta.uid() != rustix::process::geteuid().as_raw() || meta.mode() & 0o7777 != 0o700 {
        return Err(invalid("manager_external_directory_invalid"));
    }
    Ok(())
}
fn validate_file(file: &File) -> io::Result<()> {
    let meta = file.metadata()?;
    if !meta.is_file() || meta.nlink() != 1 || meta.uid() != rustix::process::geteuid().as_raw() || meta.mode() & 0o7777 != 0o600 {
        return Err(invalid("manager_external_file_invalid"));
    }
    Ok(())
}
fn child_directory(parent: &File, name: &str) -> io::Result<File> {
    match mkdirat(parent, name, Mode::RUSR | Mode::WUSR | Mode::XUSR) {
        Ok(()) | Err(rustix::io::Errno::EXIST) => (),
        Err(error) => return Err(error.into()),
    }
    let file: File = openat(parent, name, OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC, Mode::empty())?.into();
    validate_directory(&file)?;
    Ok(file)
}
fn read_optional(parent: &File, name: &str) -> io::Result<Option<Vec<u8>>> {
    let file: File = match openat(parent, name, OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC, Mode::empty()) {
        Ok(file) => file.into(),
        Err(rustix::io::Errno::NOENT) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    validate_file(&file)?;
    let mut bytes = Vec::new();
    file.take(MAX_DOCUMENT as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() > MAX_DOCUMENT {
        return Err(invalid("manager_external_file_size"));
    }
    Ok(Some(bytes))
}
fn write_document(parent: &File, name: &str, bytes: &[u8], replace: bool) -> io::Result<()> {
    if bytes.is_empty() || bytes.len() > MAX_DOCUMENT {
        return Err(invalid("manager_external_file_size"));
    }
    let mut stage: File = openat(
        parent,
        ".stage",
        OFlags::WRONLY | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?
    .into();
    validate_file(&stage)?;
    stage.set_len(0)?;
    stage.write_all(bytes)?;
    stage.sync_all()?;
    renameat_with(parent, ".stage", parent, name, if replace { RenameFlags::empty() } else { RenameFlags::NOREPLACE })?;
    parent.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};

    fn setup() -> (tempfile::TempDir, Settings, NativeIdentity) {
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let state = root.path().join("state");
        std::fs::create_dir(&state).unwrap();
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700)).unwrap();
        let settings = Settings {
            mode: ExternalMode::External,
            identity_id: "hath-external".into(),
            rpc_url: "https://node.example/".into(),
            credential_file: root.path().join("token"),
            spool_directory: state,
            download_root: root.path().join("downloads"),
            allow_private_http: false,
            ca_certificate_file: None,
            heartbeat_millis: 30_000,
            snapshot_millis: 10_000,
        };
        let fixture: serde_json::Value = serde_json::from_str(include_str!("fixtures/agent-registration-external-rust-v1.json")).unwrap();
        let identity = serde_json::from_value(fixture["nativeIdentity"].clone()).unwrap();
        (root, settings, identity)
    }

    #[test]
    fn restarts_preserve_original_contract_and_never_relabel_old_boot() {
        let (_root, settings, mut identity) = setup();
        let first = Registry::start(&settings, &identity).unwrap();
        assert_eq!(first.current.generation, 1);
        assert!(first.history.is_empty());
        let previous = first.current.clone();
        assert_eq!(Registry::start(&settings, &identity).err().unwrap().to_string(), "manager_external_profile_busy");
        drop(first);
        identity.implementation_version = "1.20.0".into();
        identity.build_commit = "c".repeat(40);
        let second = Registry::start(&settings, &identity).unwrap();
        assert_eq!(second.current.generation, 2);
        assert_ne!(second.current.instance_id, previous.instance_id);
        assert_eq!(second.history[0].registration, previous.registration);
        let replay = second.history[0].replay_registration();
        assert_eq!(replay["nativeIdentity"]["implementationVersion"], "1.19.2");
        assert_eq!(replay["externalIdentity"]["purpose"], "replay");
        assert_eq!(replay["instanceId"], previous.instance_id);
        assert_eq!(second.current.registration["nativeIdentity"]["implementationVersion"], "1.20.0");
    }

    #[test]
    fn binding_changes_missing_binding_and_corrupt_history_are_not_reset() {
        let (_root, mut settings, identity) = setup();
        let boot = Registry::start(&settings, &identity).unwrap().current;
        settings.identity_id = "other".into();
        assert_eq!(Registry::start(&settings, &identity).err().unwrap().to_string(), "manager_external_binding_changed");
        settings.identity_id = "hath-external".into();
        let path = settings.spool_directory.join("boots").join(boot.name());
        std::fs::write(&path, "corrupt").unwrap();
        assert!(Registry::start(&settings, &identity).is_err());
        assert_eq!(std::fs::read_to_string(path).unwrap(), "corrupt");
        std::fs::remove_file(settings.spool_directory.join("binding.json")).unwrap();
        assert_eq!(Registry::start(&settings, &identity).err().unwrap().to_string(), "manager_external_binding_missing");
    }

    #[test]
    fn unpublished_stage_is_bounded_and_does_not_reuse_committed_generation() {
        let (_root, settings, identity) = setup();
        drop(Registry::start(&settings, &identity).unwrap());
        let stage = settings.spool_directory.join("boots/.stage");
        std::fs::write(&stage, "interrupted").unwrap();
        std::fs::set_permissions(&stage, std::fs::Permissions::from_mode(0o600)).unwrap();
        let next = Registry::start(&settings, &identity).unwrap();
        assert_eq!(next.current.generation, 2);
        assert!(!stage.exists());
        drop(next);
        symlink(settings.spool_directory.join("binding.json"), &stage).unwrap();
        assert!(Registry::start(&settings, &identity).is_err());
    }

    #[test]
    fn external_configuration_requires_mode_and_disjoint_canonical_roots() {
        let (_root, mut settings, _) = setup();
        settings.validate().unwrap();
        settings.download_root = settings.spool_directory.join("downloads");
        assert!(settings.validate().is_err());
        settings.download_root = "/downloads/../secrets".into();
        assert!(settings.validate().is_err());
        settings.download_root = "/".into();
        assert!(settings.validate().is_err());
        assert!(protocol::decode::<Settings>(br#"{"mode":"managed"}"#).is_err());
    }

    #[test]
    fn missing_latest_snapshot_never_rewinds_generation() {
        let (_root, settings, identity) = setup();
        let first = Registry::start(&settings, &identity).unwrap().current;
        std::fs::remove_file(settings.spool_directory.join("boots").join(first.name())).unwrap();
        let next = Registry::start(&settings, &identity).unwrap();
        assert_eq!(next.current.generation, 2);
        assert_ne!(next.current.instance_id, first.instance_id);
        assert!(next.history.is_empty()); // Never invent a replay contract for a lost boot.
    }

    #[test]
    fn orphaned_outbox_requires_original_snapshot_instead_of_new_identity() {
        let (_root, settings, identity) = setup();
        let boot = Registry::start(&settings, &identity).unwrap().current;
        std::fs::create_dir(settings.spool_directory.join("outbox").join(&boot.instance_id)).unwrap();
        std::fs::remove_file(settings.spool_directory.join("boots").join(boot.name())).unwrap();
        assert_eq!(Registry::start(&settings, &identity).err().unwrap().to_string(), "manager_external_boot_missing");
    }
}

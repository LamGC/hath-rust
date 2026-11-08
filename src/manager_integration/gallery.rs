//! Private Gallery handoff transaction. Only this module knows metadata/journal JSON.
//! A prepared journal is evidence of an explicit terminal decision, never inferred from files.
use super::{
    ADAPTER_ID, INTEGRATION_VERSION,
    config::{canonical_id, invalid, open_no_follow},
    outbox::{Event, Reservation},
    protocol,
    runtime::IntegrationHandle,
};
use aws_lc_rs::digest;
use rustix::fs::{AtFlags, Mode, OFlags, RenameFlags, mkdirat, openat, renameat_with, unlinkat};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, Read, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
};

const MARKER: &str = ".hath-manager.download.json";
const JOURNAL: &str = ".hath-manager.download-handoff.json";
const PENDING: &str = ".hath-manager.download-metadata.pending";
const METADATA: &str = ".hath-manager.json";
const MAX_METADATA: usize = 16 * 1024 * 1024;

pub(crate) struct FileView<'a> {
    pub page: usize,
    pub filename: &'a str,
    pub extension: &'a str,
    pub expected_sha1: Option<[u8; 20]>,
}
pub(crate) trait GalleryView {
    fn gid(&self) -> i32;
    fn resolution(&self) -> &str;
    fn title(&self) -> &str;
    fn information(&self) -> &str;
    fn file_count(&self) -> usize;
    fn files(&self) -> impl Iterator<Item = FileView<'_>>;
}
pub(crate) struct FileReceipt {
    pub page: usize,
    pub size: u64,
}

struct Description {
    gid: i32,
    resolution: String,
    title: String,
    information: String,
    files: Vec<PlannedFile>,
}
struct PlannedFile {
    page: usize,
    path: String,
    expected_sha1: Option<String>,
}
impl Description {
    fn capture(view: &impl GalleryView) -> io::Result<Self> {
        if view.gid() <= 0
            || !(1..=10000).contains(&view.file_count())
            || view.resolution().len() > 10
            || !(view.resolution() == "org" || !view.resolution().is_empty() && view.resolution().bytes().all(|b| b.is_ascii_digit()))
            || view.title().is_empty()
            || view.title().len() > 4096
            || view.title().chars().any(char::is_control)
            || view.information().len() > 8192
            || view.information().contains('\0')
        {
            return Err(invalid("gallery_view_invalid"));
        }
        let mut files = Vec::new();
        let mut pages = HashSet::new();
        let mut paths = HashSet::new();
        for file in view.files() {
            let path = format!("{}.{}", file.filename, file.extension);
            if files.len() >= view.file_count()
                || file.page == 0
                || file.page > view.file_count()
                || !valid_image_path(&path)
                || !pages.insert(file.page)
                || !paths.insert(path.to_ascii_lowercase())
            {
                return Err(invalid("gallery_view_file_invalid"));
            }
            files.push(PlannedFile {
                page: file.page,
                path,
                expected_sha1: file.expected_sha1.map(const_hex::encode),
            });
        }
        if files.len() != view.file_count() {
            return Err(invalid("gallery_view_count"));
        }
        Ok(Self {
            gid: view.gid(),
            resolution: view.resolution().into(),
            title: view.title().into(),
            information: view.information().into(),
            files,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Journal {
    schema_version: u8,
    instance_id: String,
    attempt_id: String,
    directory: PathBuf,
    initialized: Event,
    terminal_seed: Event,
    terminal: Option<Terminal>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Terminal {
    ready: bool,
    metadata_sha256: String,
    event: Event,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Metadata {
    schema_version: u8,
    job_key: String,
    status: String,
    gid: i32,
    gallery_token: Option<String>,
    min_x_res: String,
    title: String,
    information: String,
    hath: Hath,
    agent: Agent,
    download: Download,
    files: Vec<MetadataFile>,
    download_attempt_id: String,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Hath {
    client_version: String,
    client_build: Option<i32>,
    jar_sha256: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Agent {
    version: String,
    adapter_id: String,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Download {
    directory: PathBuf,
    completed_at: chrono::DateTime<chrono::Utc>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MetadataFile {
    page: usize,
    relative_path: String,
    expected_sha1: Option<String>,
    size: u64,
}

pub(crate) struct GalleryAttempt {
    handle: IntegrationHandle,
    description: Arc<Description>,
    journal: Journal,
    ready: Option<Event>,
    last_active: tokio::time::Instant,
}
impl IntegrationHandle {
    pub(crate) fn prepare_gallery(&self, base_directory: &Path, view: &impl GalleryView) -> io::Result<Option<GalleryAttempt>> {
        if !self.enabled() {
            return Ok(None);
        }
        if !base_directory.is_absolute() {
            return Err(invalid("gallery_absolute_directory_required"));
        }
        let description = Arc::new(Description::capture(view)?);
        let attempt = super::new_id()?;
        let directory = base_directory.join(format!("attempt-{attempt}"));
        if directory.to_str().is_none_or(|value| value.len() > 4096) {
            return Err(invalid("gallery_directory_limit"));
        }
        let initialized = self
            .event("gallery.download.initialized", payload(&description, &directory, &attempt))?
            .ok_or_else(|| invalid("gallery_integration_disabled"))?;
        let terminal_seed = self
            .event("gallery.download.completed", json!({}))?
            .ok_or_else(|| invalid("gallery_integration_disabled"))?;
        let journal = Journal {
            schema_version: 1,
            instance_id: initialized.instance_id.clone(),
            attempt_id: attempt,
            directory,
            initialized,
            terminal_seed,
            terminal: None,
        };
        Ok(Some(GalleryAttempt {
            handle: self.clone(),
            description,
            journal,
            ready: None,
            last_active: tokio::time::Instant::now(),
        }))
    }
}
impl GalleryAttempt {
    pub(crate) fn directory(&self) -> &Path {
        &self.journal.directory
    }

    pub(crate) async fn initialize(&mut self) -> io::Result<()> {
        let journal = self.journal.clone();
        self.checked(
            tokio::task::spawn_blocking(move || {
                let directory = Directory::create(&journal.directory)?;
                directory.immutable(JOURNAL, &encode(&journal, 64 * 1024)?)
            })
            .await
            .map_err(|_| invalid("gallery_persistence_worker"))?,
        )?;
        self.handle
            .reserve(
                Reservation::new(&self.journal.initialized, &self.journal.attempt_id, false)?,
                Reservation::new(&self.journal.terminal_seed, &self.journal.attempt_id, true)?,
            )
            .await?;
        let journal = self.journal.clone();
        self.checked(
            tokio::task::spawn_blocking(move || {
                Directory::open(&journal.directory)?.immutable(MARKER, &encode(&journal.initialized.payload, 16 * 1024)?)
            })
            .await
            .map_err(|_| invalid("gallery_persistence_worker"))?,
        )?;
        self.handle.persist(&self.journal.initialized).await
    }

    pub(crate) async fn active(&mut self) {
        if self.last_active.elapsed() < std::time::Duration::from_secs(30) || self.ready.is_some() {
            return;
        }
        self.last_active = tokio::time::Instant::now();
        if !self
            .handle
            .health()
            .is_some_and(|health| health.link_healthy && health.persistence_healthy && health.quarantined == 0)
        {
            return;
        }
        if let Ok(Some(event)) = self
            .handle
            .event("gallery.download.active", self.journal.initialized.payload.clone())
        {
            let _ = self.handle.persist(&event).await; // Activity may be dropped; terminal decisions may not.
        }
    }

    pub(crate) async fn finish(&mut self, success: bool, receipts: Vec<FileReceipt>) -> io::Result<()> {
        if self.ready.is_none() {
            let journal = self.journal.clone();
            let description = self.description.clone();
            let mut work = tokio::task::spawn_blocking(move || prepare_terminal(&journal, &description, success, receipts));
            let result = loop {
                tokio::select! { biased; result = &mut work => break result.map_err(|_| invalid("gallery_persistence_worker"))?,
                _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => self.active().await }
            };
            self.ready = Some(self.checked(result)?);
        }
        let event = self.ready.as_ref().unwrap();
        if (event.event_type == "gallery.download.completed") != success {
            return Err(invalid("gallery_terminal_conflict"));
        }
        self.handle.persist(event).await
    }

    fn checked<T>(&self, result: io::Result<T>) -> io::Result<T> {
        if result.is_err() {
            self.handle.persistence_failed();
        }
        result
    }
}

fn payload(description: &Description, directory: &Path, attempt: &str) -> Value {
    json!({"eventVersion":2,"downloadAttemptId":attempt,"jobKey":format!("{};{}",description.gid,description.resolution),
        "gid":description.gid,"minXRes":description.resolution,"title":description.title,"fileCount":description.files.len(),"downloadDirectory":directory})
}
fn encode(value: &impl Serialize, maximum: usize) -> io::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|_| invalid("gallery_json_invalid"))?;
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(invalid("gallery_json_limit"));
    }
    Ok(bytes)
}
fn sha256(bytes: &[u8]) -> String {
    const_hex::encode(digest::digest(&digest::SHA256, bytes))
}

fn prepare_terminal(expected: &Journal, description: &Description, success: bool, receipts: Vec<FileReceipt>) -> io::Result<Event> {
    prepare_terminal_inner(expected, description, success, receipts, &mut |_| Ok(()))
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Milestone {
    PendingWritten,
    PreparedWritten,
    MetadataCommitted,
    ReadyWritten,
}

fn prepare_terminal_inner(
    expected: &Journal,
    description: &Description,
    success: bool,
    receipts: Vec<FileReceipt>,
    checkpoint: &mut impl FnMut(Milestone) -> io::Result<()>,
) -> io::Result<Event> {
    let directory = Directory::open(&expected.directory)?;
    let mut journal = load_journal(&directory, &expected.instance_id)?;
    if journal.initialized != expected.initialized
        || journal.terminal_seed != expected.terminal_seed
        || journal.attempt_id != expected.attempt_id
    {
        return Err(invalid("gallery_journal_conflict"));
    }
    if journal.terminal.is_none() {
        let mut sizes = HashMap::new();
        for receipt in receipts {
            if receipt.size == 0 || receipt.size > i64::MAX as u64 || sizes.insert(receipt.page, receipt.size).is_some() {
                return Err(invalid("gallery_receipt_invalid"));
            }
        }
        if success && sizes.len() != description.files.len() {
            return Err(invalid("gallery_receipt_incomplete"));
        }
        let metadata = match directory.read_optional(PENDING, MAX_METADATA)? {
            Some(bytes) => bytes,
            None => {
                let mut files = Vec::new();
                if success {
                    for planned in &description.files {
                        let size = *sizes.get(&planned.page).ok_or_else(|| invalid("gallery_receipt_incomplete"))?;
                        directory.verify_image(&planned.path, size, planned.expected_sha1.as_deref())?;
                        files.push(MetadataFile {
                            page: planned.page,
                            relative_path: planned.path.clone(),
                            expected_sha1: planned.expected_sha1.clone(),
                            size,
                        });
                    }
                    directory.immutable("galleryinfo.txt", description.information.as_bytes())?;
                }
                let metadata = Metadata {
                    schema_version: 2,
                    job_key: format!("{};{}", description.gid, description.resolution),
                    status: if success { "completed" } else { "failed" }.into(),
                    gid: description.gid,
                    gallery_token: None,
                    min_x_res: description.resolution.clone(),
                    title: description.title.clone(),
                    information: description.information.clone(),
                    hath: Hath {
                        client_version: env!("CARGO_PKG_VERSION").into(),
                        client_build: None,
                        jar_sha256: None,
                    },
                    agent: Agent {
                        version: INTEGRATION_VERSION.into(),
                        adapter_id: ADAPTER_ID.into(),
                    },
                    download: Download {
                        directory: journal.directory.clone(),
                        completed_at: chrono::Utc::now(),
                    },
                    files,
                    download_attempt_id: journal.attempt_id.clone(),
                };
                let bytes = encode(&metadata, MAX_METADATA)?;
                directory.immutable(PENDING, &bytes)?;
                checkpoint(Milestone::PendingWritten)?;
                bytes
            }
        };
        let document: Metadata = protocol::decode_limit(&metadata, MAX_METADATA).map_err(invalid)?;
        if (document.status == "completed") != success
            || document.information != description.information
            || success
                && (document.files.len() != description.files.len()
                    || document.files.iter().zip(&description.files).any(|(file, planned)| {
                        file.page != planned.page
                            || file.relative_path != planned.path
                            || file.expected_sha1 != planned.expected_sha1
                            || sizes.get(&file.page) != Some(&file.size)
                    }))
        {
            return Err(invalid("gallery_terminal_conflict"));
        }
        let mut event = journal.terminal_seed.clone();
        event.event_type = if success {
            "gallery.download.completed"
        } else {
            "gallery.download.failed"
        }
        .into();
        event.occurred_at = document.download.completed_at;
        event.payload = journal.initialized.payload.clone();
        event.payload["success"] = json!(success);
        event.payload["metadataPath"] = json!(journal.directory.join(METADATA));
        event.payload["metadataSha256"] = json!(sha256(&metadata));
        event.encode()?;
        journal.terminal = Some(Terminal {
            ready: false,
            metadata_sha256: sha256(&metadata),
            event,
        });
        // This is the durable terminal decision; staged files alone never authorize replay.
        directory.replace(JOURNAL, &encode(&journal, 64 * 1024)?)?;
        checkpoint(Milestone::PreparedWritten)?;
    }
    let event = recover_commit(&directory, &mut journal, checkpoint)?.ok_or_else(|| invalid("gallery_terminal_missing"))?;
    if (event.event_type == "gallery.download.completed") != success {
        return Err(invalid("gallery_terminal_conflict"));
    }
    Ok(event)
}

/// Local recovery primitive for a fenced, known run. Never changes instanceId or event identity.
/// The native driver/importer will call this only after verifying the run is no longer writing.
pub(super) fn recover_ready(directory: &Path, expected_instance: &str) -> io::Result<Option<Event>> {
    Ok(recover_handoff(directory, expected_instance)?.1)
}

// Caller owns the spool-root writer lock and has fenced the original process. No network/runtime is created.
pub(super) fn recover_handoff(directory: &Path, expected_instance: &str) -> io::Result<(Event, Option<Event>)> {
    let directory = Directory::open(directory)?;
    let mut journal = load_journal(&directory, expected_instance)?;
    let terminal = recover_commit(&directory, &mut journal, &mut |_| Ok(()))?;
    Ok((journal.initialized, terminal))
}

fn load_journal(directory: &Directory, expected_instance: &str) -> io::Result<Journal> {
    let journal: Journal = protocol::decode(&directory.read(JOURNAL, 64 * 1024)?).map_err(invalid)?;
    if journal.schema_version != 1
        || journal.instance_id != expected_instance
        || !canonical_id(expected_instance)
        || !canonical_id(&journal.attempt_id)
        || journal.directory != directory.path
        || journal.initialized.instance_id != expected_instance
        || journal.terminal_seed.instance_id != expected_instance
        || journal.initialized.payload["downloadAttemptId"].as_str() != Some(&journal.attempt_id)
        || journal.initialized.payload["downloadDirectory"].as_str() != directory.path.to_str()
        || journal.initialized.event_type != "gallery.download.initialized"
        || journal.initialized.event_id == journal.terminal_seed.event_id
        || journal.initialized.sequence >= journal.terminal_seed.sequence
        || !directory
            .path
            .file_name()
            .is_some_and(|name| name == format!("attempt-{}", journal.attempt_id).as_str())
    {
        return Err(invalid("gallery_journal_identity"));
    }
    journal.initialized.encode()?;
    journal.terminal_seed.encode()?;
    Ok(journal)
}

fn recover_commit(
    directory: &Directory,
    journal: &mut Journal,
    checkpoint: &mut impl FnMut(Milestone) -> io::Result<()>,
) -> io::Result<Option<Event>> {
    let Some(terminal) = &journal.terminal else {
        return Ok(None);
    };
    let bytes = match directory.read_optional(METADATA, MAX_METADATA)? {
        Some(bytes) => bytes,
        None if !terminal.ready => directory.read(PENDING, MAX_METADATA)?,
        None => return Err(invalid("gallery_ready_metadata_missing")),
    };
    if sha256(&bytes) != terminal.metadata_sha256 {
        return Err(invalid("gallery_metadata_digest"));
    }
    let metadata: Metadata = protocol::decode_limit(&bytes, MAX_METADATA).map_err(invalid)?;
    let success = metadata.status == "completed";
    let mut expected = journal.terminal_seed.clone();
    expected.event_type = if success {
        "gallery.download.completed"
    } else {
        "gallery.download.failed"
    }
    .into();
    expected.occurred_at = metadata.download.completed_at;
    expected.payload = journal.initialized.payload.clone();
    expected.payload["success"] = json!(success);
    expected.payload["metadataPath"] = json!(directory.path.join(METADATA));
    expected.payload["metadataSha256"] = json!(terminal.metadata_sha256);
    if terminal.event != expected
        || !matches!(metadata.status.as_str(), "completed" | "failed")
        || metadata.schema_version != 2
        || metadata.download_attempt_id != journal.attempt_id
        || metadata.download.directory != directory.path
        || metadata.job_key != journal.initialized.payload["jobKey"].as_str().unwrap_or("")
        || metadata.gid <= 0
        || Some(i64::from(metadata.gid)) != journal.initialized.payload["gid"].as_i64()
        || Some(metadata.title.as_str()) != journal.initialized.payload["title"].as_str()
        || Some(metadata.min_x_res.as_str()) != journal.initialized.payload["minXRes"].as_str()
        || metadata.gallery_token.is_some()
        || metadata.information.len() > 8192
        || metadata.hath.client_version != env!("CARGO_PKG_VERSION")
        || metadata.hath.client_build.is_some()
        || metadata.hath.jar_sha256.is_some()
        || metadata.agent.version != INTEGRATION_VERSION
        || metadata.agent.adapter_id != ADAPTER_ID
        || success && (metadata.files.is_empty() || Some(metadata.files.len() as u64) != journal.initialized.payload["fileCount"].as_u64())
        || !success && !metadata.files.is_empty()
        || metadata.files.len() > 10000
    {
        return Err(invalid("gallery_metadata_identity"));
    }
    let mut pages = HashSet::new();
    let mut paths = HashSet::new();
    for file in &metadata.files {
        if file.page == 0
            || file.page > metadata.files.len()
            || !pages.insert(file.page)
            || !valid_image_path(&file.relative_path)
            || !paths.insert(file.relative_path.to_ascii_lowercase())
            || file.size == 0
            || file.size > i64::MAX as u64
        {
            return Err(invalid("gallery_metadata_file"));
        }
        directory.verify_image(&file.relative_path, file.size, file.expected_sha1.as_deref())?;
    }
    if success {
        directory.immutable("galleryinfo.txt", metadata.information.as_bytes())?;
    }
    directory.immutable(METADATA, &bytes)?;
    checkpoint(Milestone::MetadataCommitted)?;
    let event = terminal.event.clone();
    journal.terminal.as_mut().unwrap().ready = true;
    directory.replace(JOURNAL, &encode(journal, 64 * 1024)?)?;
    checkpoint(Milestone::ReadyWritten)?;
    if let Some(staged) = directory.read_optional(PENDING, MAX_METADATA)? {
        if staged != bytes {
            return Err(invalid("gallery_pending_conflict"));
        }
        unlinkat(&directory.fd, PENDING, AtFlags::empty())?;
        directory.fd.sync_all()?;
    }
    Ok(Some(event))
}

fn valid_image_path(path: &str) -> bool {
    let Some((name, extension)) = path.rsplit_once('.') else {
        return false;
    };
    !name.is_empty()
        && name.len() <= 250
        && name != "."
        && name != ".."
        && !name.to_ascii_lowercase().starts_with(".hath-manager")
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        && matches!(extension, "jpg" | "png" | "gif" | "webp")
}

struct Directory {
    fd: File,
    path: PathBuf,
}
impl Directory {
    fn open(path: &Path) -> io::Result<Self> {
        let fd = open_no_follow(path)?;
        validate_dir(&fd, true)?;
        Ok(Self { fd, path: path.into() })
    }
    fn create(path: &Path) -> io::Result<Self> {
        let base = path.parent().ok_or_else(|| invalid("gallery_directory"))?;
        let root = open_no_follow(base.parent().ok_or_else(|| invalid("gallery_directory"))?)?;
        validate_dir(&root, false)?;
        let name = base.file_name().ok_or_else(|| invalid("gallery_directory"))?;
        let parent = create_child(&root, name, false)?;
        root.sync_all()?;
        let fd = create_child(&parent, path.file_name().ok_or_else(|| invalid("gallery_directory"))?, true)?;
        parent.sync_all()?;
        Ok(Self { fd, path: path.into() })
    }
    fn read_optional(&self, name: &str, maximum: usize) -> io::Result<Option<Vec<u8>>> {
        match self.read(name, maximum) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
    fn read(&self, name: &str, maximum: usize) -> io::Result<Vec<u8>> {
        let file = self.file(name)?;
        if file.metadata()?.len() > maximum as u64 {
            return Err(invalid("gallery_file_limit"));
        }
        let mut bytes = Vec::new();
        file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > maximum {
            return Err(invalid("gallery_file_limit"));
        }
        Ok(bytes)
    }
    fn file(&self, name: &str) -> io::Result<File> {
        let file: File =
            openat(&self.fd, name, OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK, Mode::empty())?.into();
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.uid() != rustix::process::geteuid().as_raw() || metadata.mode() & 0o7777 != 0o600 {
            return Err(invalid("gallery_private_file"));
        }
        Ok(file)
    }
    fn immutable(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        if let Some(existing) = self.read_optional(name, bytes.len())? {
            return if existing == bytes {
                self.fd.sync_all()
            } else {
                Err(invalid("gallery_immutable_conflict"))
            };
        }
        self.write(name, bytes, false)
    }
    fn replace(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        self.write(name, bytes, true)
    }
    fn write(&self, name: &str, bytes: &[u8], replace: bool) -> io::Result<()> {
        // One stable stage per private target: failed cleanup must not create unlimited copies.
        // Only a single fenced Gallery transaction may write this directory.
        let temporary = format!(".hath-manager.download-stage-{name}");
        let result = (|| {
            let mut file: File = openat(
                &self.fd,
                temporary.as_str(),
                OFlags::WRONLY | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )?
            .into();
            let metadata = file.metadata()?;
            if !metadata.is_file()
                || metadata.uid() != rustix::process::geteuid().as_raw()
                || metadata.mode() & 0o7777 != 0o600
                || metadata.nlink() != 1
            {
                return Err(invalid("gallery_private_stage"));
            }
            file.set_len(0)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            renameat_with(
                &self.fd,
                temporary.as_str(),
                &self.fd,
                name,
                if replace { RenameFlags::empty() } else { RenameFlags::NOREPLACE },
            )?;
            self.fd.sync_all()
        })();
        if result.is_err() {
            let _ = unlinkat(&self.fd, temporary.as_str(), AtFlags::empty());
        }
        result
    }
    fn verify_image(&self, name: &str, size: u64, expected: Option<&str>) -> io::Result<()> {
        let mut file = self.file(name)?;
        if file.metadata()?.len() != size {
            return Err(invalid("gallery_image_size"));
        }
        if let Some(expected) = expected {
            if expected.len() != 40 || !expected.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(invalid("gallery_image_hash"));
            }
            let mut hasher = digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY);
            let mut buffer = [0; 64 * 1024];
            loop {
                let length = file.read(&mut buffer)?;
                if length == 0 {
                    break;
                }
                hasher.update(&buffer[..length]);
            }
            if const_hex::encode(hasher.finish()) != expected.to_ascii_lowercase() {
                return Err(invalid("gallery_image_hash"));
            }
        }
        file.sync_all()?;
        if file.metadata()?.len() != size {
            return Err(invalid("gallery_image_size"));
        }
        Ok(())
    }
}
fn validate_dir(file: &File, private: bool) -> io::Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_dir()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || if private {
            metadata.mode() & 0o7777 != 0o700
        } else {
            metadata.mode() & 0o022 != 0
        }
    {
        return Err(invalid("gallery_private_directory"));
    }
    Ok(())
}
fn create_child(parent: &File, name: &std::ffi::OsStr, private: bool) -> io::Result<File> {
    match mkdirat(parent, name, Mode::RUSR | Mode::WUSR | Mode::XUSR) {
        Ok(()) => (),
        Err(rustix::io::Errno::EXIST) => (),
        Err(error) => return Err(error.into()),
    }
    let fd: File = openat(parent, name, OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC, Mode::empty())?.into();
    validate_dir(&fd, private)?;
    Ok(fd)
}

#[cfg(test)]
mod tests {
    use super::super::{
        config::Config,
        outbox::{Limits, Outbox},
        protocol::NativeIdentity,
        runtime::Runtime,
    };
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};

    pub(super) struct TestGallery;
    impl GalleryView for TestGallery {
        fn gid(&self) -> i32 {
            2645625
        }
        fn resolution(&self) -> &str {
            "org"
        }
        fn title(&self) -> &str {
            "Example Gallery"
        }
        fn information(&self) -> &str {
            "Example information\n"
        }
        fn file_count(&self) -> usize {
            2
        }
        fn files(&self) -> impl Iterator<Item = FileView<'_>> {
            [
                FileView {
                    page: 1,
                    filename: "001",
                    extension: "jpg",
                    expected_sha1: Some(
                        digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, b"image-one")
                            .as_ref()
                            .try_into()
                            .unwrap(),
                    ),
                },
                FileView {
                    page: 2,
                    filename: "002",
                    extension: "png",
                    expected_sha1: None,
                },
            ]
            .into_iter()
        }
    }
    async fn runtime(root: &Path) -> (Runtime, tokio::net::TcpListener, PathBuf) {
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let token = root.join("token");
        std::fs::write(&token, "A".repeat(43)).unwrap();
        std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
        let spool = root.join("spool");
        std::fs::create_dir(&spool).unwrap();
        std::fs::set_permissions(&spool, std::fs::Permissions::from_mode(0o700)).unwrap();
        let fixture: Value = serde_json::from_str(include_str!("fixtures/agent-registration-native-v1.json")).unwrap();
        let identity: NativeIdentity = serde_json::from_value(fixture["nativeIdentity"].clone()).unwrap();
        let config = Config {
            instance_id: "11111111-1111-4111-8111-111111111111".into(),
            stable_instance_key: "primary".into(),
            rpc_url: format!("http://{}/", listener.local_addr().unwrap()),
            credential_file: token,
            spool_directory: spool.clone(),
            heartbeat_millis: 1000,
            snapshot_millis: 1000,
        };
        (Runtime::start(config, identity).await.unwrap(), listener, spool)
    }
    fn images(attempt: &GalleryAttempt) {
        let directory = Directory::open(attempt.directory()).unwrap();
        directory.immutable("001.jpg", b"image-one").unwrap();
        directory.immutable("002.png", b"image-two").unwrap();
    }
    fn receipts() -> Vec<FileReceipt> {
        vec![FileReceipt { page: 1, size: 9 }, FileReceipt { page: 2, size: 9 }]
    }

    #[tokio::test]
    async fn actual_metadata_writer_matches_shared_native_fixture_and_retry_keeps_identity_and_bytes() {
        let root = tempfile::tempdir().unwrap();
        let (runtime, _listener, spool) = runtime(root.path()).await;
        let handle = runtime.handle();
        let mut attempt = handle.prepare_gallery(&root.path().join("gallery"), &TestGallery).unwrap().unwrap();
        attempt.initialize().await.unwrap();
        images(&attempt);
        attempt.finish(true, receipts()).await.unwrap();
        let original = std::fs::read(attempt.directory().join(METADATA)).unwrap();
        let mut value: Value = serde_json::from_slice(&original).unwrap();
        value["download"]["directory"] = json!("${GALLERY_DIRECTORY}");
        value["download"]["completedAt"] = json!("2026-11-07T00:00:00Z");
        value["downloadAttemptId"] = json!("11111111-2222-4333-8444-555555555555");
        let fixture: Value = serde_json::from_str(include_str!("fixtures/gallery-metadata-v2-rust-completed.json.template")).unwrap();
        let provenance: Value = serde_json::from_str(include_str!("fixtures/provenance.json")).unwrap();
        assert_eq!(
            provenance["galleryMetadata"]["sha256"],
            sha256(include_bytes!("fixtures/gallery-metadata-v2-rust-completed.json.template"))
        );
        assert_eq!(value, fixture);
        let event = attempt.ready.clone().unwrap();
        attempt.finish(true, Vec::new()).await.unwrap();
        assert!(attempt.ready.as_ref().unwrap() == &event);
        assert_eq!(std::fs::read(attempt.directory().join(METADATA)).unwrap(), original);
        assert!(attempt.finish(false, Vec::new()).await.is_err());
        assert_eq!(event.payload["metadataSha256"], sha256(&original));
        assert!(recover_ready(attempt.directory(), &event.instance_id).unwrap().unwrap() == event);
        assert!(recover_ready(attempt.directory(), "22222222-2222-4222-8222-222222222222").is_err());
        runtime.shutdown().await.unwrap();
        let outbox = Outbox::open(&spool, &event.instance_id, Limits::default()).unwrap();
        let events = outbox.batch().unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().any(|item| item.event == event));
    }

    #[tokio::test]
    async fn failure_metadata_has_no_allowlist_and_each_same_gallery_attempt_is_isolated() {
        let root = tempfile::tempdir().unwrap();
        let (runtime, _listener, _) = runtime(root.path()).await;
        let handle = runtime.handle();
        let mut first = handle.prepare_gallery(&root.path().join("gallery"), &TestGallery).unwrap().unwrap();
        let mut second = handle.prepare_gallery(&root.path().join("gallery"), &TestGallery).unwrap().unwrap();
        first.initialize().await.unwrap();
        second.initialize().await.unwrap();
        assert_ne!(first.directory(), second.directory());
        Directory::open(first.directory())
            .unwrap()
            .immutable("001.jpg", b"partial")
            .unwrap();
        first.finish(false, Vec::new()).await.unwrap();
        let metadata: Value = serde_json::from_slice(&std::fs::read(first.directory().join(METADATA)).unwrap()).unwrap();
        assert_eq!(metadata["status"], "failed");
        assert_eq!(metadata["files"], json!([]));
        assert!(!second.directory().join(METADATA).exists());
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn every_terminal_commit_boundary_recovers_only_after_explicit_durable_decision() {
        let root = tempfile::tempdir().unwrap();
        let (runtime, _listener, _) = runtime(root.path()).await;
        for boundary in [
            Milestone::PendingWritten,
            Milestone::PreparedWritten,
            Milestone::MetadataCommitted,
            Milestone::ReadyWritten,
        ] {
            let mut attempt = runtime
                .handle()
                .prepare_gallery(&root.path().join("gallery"), &TestGallery)
                .unwrap()
                .unwrap();
            attempt.initialize().await.unwrap();
            images(&attempt);
            let result = prepare_terminal_inner(&attempt.journal, &attempt.description, true, receipts(), &mut |point| {
                if point == boundary {
                    Err(invalid("injected_process_interruption"))
                } else {
                    Ok(())
                }
            });
            assert!(result.is_err(), "{boundary:?}");
            let recovered = recover_ready(attempt.directory(), &attempt.journal.instance_id).unwrap();
            if boundary == Milestone::PendingWritten {
                assert!(recovered.is_none());
                assert!(!attempt.directory().join(METADATA).exists());
                let directory = Directory::open(attempt.directory()).unwrap();
                let original = directory.read(PENDING, MAX_METADATA).unwrap();
                let mut altered: Value = serde_json::from_slice(&original).unwrap();
                altered["files"][0]["relativePath"] = json!("different.jpg");
                directory.immutable("different.jpg", b"image-one").unwrap();
                directory.replace(PENDING, &serde_json::to_vec(&altered).unwrap()).unwrap();
                assert!(attempt.finish(true, receipts()).await.is_err());
                assert!(recover_ready(attempt.directory(), &attempt.journal.instance_id).unwrap().is_none());
                directory.replace(PENDING, &original).unwrap();
            } else {
                let event = recovered.unwrap();
                assert_eq!(event.event_id, attempt.journal.terminal_seed.event_id);
                assert_eq!(event.sequence, attempt.journal.terminal_seed.sequence);
                assert!(recover_ready(attempt.directory(), &attempt.journal.instance_id).unwrap().unwrap() == event);
                runtime.handle().persist(&event).await.unwrap();
            }
            // Retrying the original explicit terminal request also handles staged-only interruption.
            attempt.finish(true, receipts()).await.unwrap();
        }
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn receipts_hashes_sizes_and_links_are_verified_before_any_success_handoff() {
        let root = tempfile::tempdir().unwrap();
        let (runtime, _listener, _) = runtime(root.path()).await;
        let mut attempt = runtime
            .handle()
            .prepare_gallery(&root.path().join("gallery"), &TestGallery)
            .unwrap()
            .unwrap();
        attempt.initialize().await.unwrap();
        assert!(recover_ready(attempt.directory(), &attempt.journal.instance_id).unwrap().is_none());
        assert!(attempt.finish(true, receipts()).await.is_err()); // missing images
        images(&attempt);
        assert!(attempt.finish(true, vec![FileReceipt { page: 1, size: 9 }]).await.is_err());
        std::fs::write(attempt.directory().join("001.jpg"), b"wrong-one").unwrap();
        assert!(attempt.finish(true, receipts()).await.is_err());
        assert!(!attempt.directory().join(METADATA).exists());
        std::fs::write(attempt.directory().join("001.jpg"), b"image-one").unwrap();
        let stage = attempt.directory().join(format!(".hath-manager.download-stage-{PENDING}"));
        symlink(root.path().join("token"), &stage).unwrap();
        assert!(attempt.finish(true, receipts()).await.is_err());
        assert_eq!(std::fs::read(root.path().join("token")).unwrap(), "A".repeat(43).as_bytes());
        // The writer rejects symlinks and its failed operation can safely remove only the stage link.
        if stage.symlink_metadata().is_ok() {
            std::fs::remove_file(&stage).unwrap();
        }
        let directory = Directory::open(attempt.directory()).unwrap();
        directory
            .immutable(&format!(".hath-manager.download-stage-{PENDING}"), b"interrupted stage")
            .unwrap();
        attempt.finish(true, receipts()).await.unwrap();
        assert!(!stage.exists());
        let target = attempt.directory().join(METADATA);
        let bytes = std::fs::read(&target).unwrap();
        std::fs::write(&target, b"changed").unwrap();
        assert!(recover_ready(attempt.directory(), &attempt.journal.instance_id).is_err());
        std::fs::write(&target, &bytes).unwrap();
        std::fs::remove_file(&target).unwrap();
        symlink(root.path().join("token"), &target).unwrap();
        assert!(recover_ready(attempt.directory(), &attempt.journal.instance_id).is_err());
        runtime.shutdown().await.unwrap();
    }

    #[test]
    fn disabled_facade_does_not_validate_views_or_touch_files() {
        assert!(
            IntegrationHandle::default()
                .prepare_gallery(Path::new("relative-unused"), &TestGallery)
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn unavailable_runtime_is_not_noop_and_cannot_release_gallery_work() {
        let directory = tempfile::tempdir().unwrap();
        let handle = IntegrationHandle::unavailable();
        assert!(handle.enabled());
        assert!(!handle.health().unwrap().persistence_healthy);
        assert!(handle.prepare_gallery(directory.path(), &TestGallery).is_err());
        assert!(handle.snapshot(json!({})).is_err());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}

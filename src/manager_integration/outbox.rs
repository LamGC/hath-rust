//! Single-writer durable queue. These blocking operations belong on the integration's
//! persistence worker, never in an HTTP hook. All filesystem operations are fd-relative.
use super::{
    config::{canonical_id, invalid, open_no_follow},
    protocol,
};
use rustix::fs::{AtFlags, Mode, OFlags, RenameFlags, mkdirat, openat, renameat_with, unlinkat};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read, Write},
    os::fd::AsRawFd,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

const MAX_RECORD: usize = 16 * 1024;
const MAX_BATCH: usize = 256 * 1024;
const MAX_QUARANTINE_RECORDS: usize = 128;
const MAX_QUARANTINE_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Event {
    pub event_id: String,
    pub instance_id: String,
    pub sequence: i64,
    pub event_type: String,
    pub occurred_at: chrono::DateTime<chrono::Utc>,
    pub payload: serde_json::Value,
}

impl Event {
    pub fn new(instance: &str, sequence: i64, kind: &str, payload: serde_json::Value) -> io::Result<Self> {
        let event = Self {
            event_id: super::new_id()?,
            instance_id: instance.into(),
            sequence,
            event_type: kind.into(),
            occurred_at: chrono::Utc::now(),
            payload,
        };
        event.validate(instance)?;
        Ok(event)
    }
    fn validate(&self, instance: &str) -> io::Result<()> {
        if !canonical_id(&self.event_id)
            || !canonical_id(&self.instance_id)
            || self.instance_id != instance
            || self.sequence < 0
            || !self.payload.is_object()
            || self.event_type.is_empty()
            || self.event_type.len() > 64
            || !self
                .event_type
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'.' | b'_' | b'-'))
        {
            return Err(invalid("manager_event_invalid"));
        }
        Ok(())
    }
    pub fn name(&self) -> String {
        format!("{:020}_{}.json", self.sequence, self.event_id)
    }
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        self.validate(&self.instance_id)?;
        let bytes = serde_json::to_vec(self).map_err(|_| invalid("manager_event_invalid"))?;
        if bytes.len() > MAX_RECORD {
            return Err(invalid("manager_event_too_large"));
        }
        let _: Event = protocol::decode(&bytes).map_err(|_| invalid("manager_event_invalid"))?;
        Ok(bytes)
    }
}

/// A full-sized private placeholder, never a wire event. Atomically replaced in place
/// by its initialized/terminal event, so a full queue cannot steal a Gallery's handoff slot.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Reservation {
    reservation_for: String,
    instance_id: String,
    event_id: String,
    sequence: i64,
    attempt_id: String,
}
impl Reservation {
    pub fn new(event: &Event, attempt: &str, terminal: bool) -> io::Result<Self> {
        let value = Self {
            reservation_for: if terminal { "terminal" } else { "initialized" }.into(),
            instance_id: event.instance_id.clone(),
            event_id: event.event_id.clone(),
            sequence: event.sequence,
            attempt_id: attempt.into(),
        };
        value.validate(&event.instance_id)?;
        Ok(value)
    }
    fn validate(&self, instance: &str) -> io::Result<()> {
        if self.instance_id != instance
            || !canonical_id(instance)
            || !canonical_id(&self.event_id)
            || !canonical_id(&self.attempt_id)
            || self.sequence < 0
            || !matches!(self.reservation_for.as_str(), "initialized" | "terminal")
        {
            return Err(invalid("manager_reservation_invalid"));
        }
        Ok(())
    }
    fn name(&self) -> String {
        format!("{:020}_{}.json", self.sequence, self.event_id)
    }
    fn accepts(&self, event: &Event) -> bool {
        self.instance_id == event.instance_id
            && self.event_id == event.event_id
            && self.sequence == event.sequence
            && event.payload["downloadAttemptId"].as_str() == Some(&self.attempt_id)
            && event.payload["eventVersion"].as_u64() == Some(2)
            && if self.reservation_for == "terminal" {
                matches!(event.event_type.as_str(), "gallery.download.completed" | "gallery.download.failed")
            } else {
                event.event_type == "gallery.download.initialized"
            }
    }
    fn encode(&self) -> io::Result<Vec<u8>> {
        let mut bytes = serde_json::to_vec(self).map_err(|_| invalid("manager_reservation_invalid"))?;
        bytes.resize(MAX_RECORD, b' ');
        Ok(bytes)
    }
}

enum Stored {
    Event(Event),
    Reserved(Reservation),
}
impl Stored {
    fn decode(bytes: &[u8], instance: &str) -> io::Result<Self> {
        let value: serde_json::Value = protocol::decode(bytes).map_err(invalid)?;
        if value.get("reservationFor").is_some() {
            if bytes.len() != MAX_RECORD {
                return Err(invalid("manager_reservation_size"));
            }
            let reservation: Reservation = serde_json::from_value(value).map_err(|_| invalid("manager_reservation_invalid"))?;
            reservation.validate(instance)?;
            Ok(Self::Reserved(reservation))
        } else {
            let event: Event = serde_json::from_value(value).map_err(|_| invalid("manager_spool_corrupt"))?;
            event.validate(instance)?;
            Ok(Self::Event(event))
        }
    }
    fn name(&self) -> String {
        match self {
            Self::Event(value) => value.name(),
            Self::Reserved(value) => value.name(),
        }
    }
}

pub(super) struct Record {
    pub name: String,
    pub bytes: Vec<u8>,
    pub event: Event,
}
#[derive(Clone, Copy)]
pub(super) struct Limits {
    pub records: usize,
    pub bytes: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            records: 4096,
            bytes: 64 * 1024 * 1024,
        }
    }
}

pub(super) struct Outbox {
    root: File,
    directory: File,
    instance: String,
    index: BTreeMap<String, u64>,
    reserved: BTreeMap<String, Reservation>,
    limits: Limits,
    total_records: usize,
    total_bytes: u64,
    quarantined: usize,
    blocked_staging: bool,
}

impl Outbox {
    pub fn open(root: &Path, instance: &str, limits: Limits) -> io::Result<Self> {
        Self::open_inner(root, instance, limits)
    }

    pub(super) fn recover_offline(root: &Path, instance: &str) -> io::Result<usize> {
        let spool = Self::open(root, instance, Limits::default())?;
        Ok(spool.quarantined())
    }

    fn open_inner(root: &Path, instance: &str, limits: Limits) -> io::Result<Self> {
        if !canonical_id(instance) || limits.records == 0 || limits.records > 4096 || limits.bytes == 0 || limits.bytes > 64 * 1024 * 1024 {
            return Err(invalid("manager_spool_configuration"));
        }
        let root = open_no_follow(root)?;
        validate_directory(&root)?;
        rustix::fs::flock(&root, rustix::fs::FlockOperation::NonBlockingLockExclusive)?;
        let directory = child_directory(&root, instance)?;
        root.sync_all()?;
        let mut spool = Self {
            root,
            directory,
            instance: instance.into(),
            index: BTreeMap::new(),
            reserved: BTreeMap::new(),
            limits,
            total_records: 0,
            total_bytes: 0,
            quarantined: 0,
            blocked_staging: false,
        };
        // Count unsafe or oversized entries before attempting any recovery or rename.
        spool.refresh_budget_inner(true)?;
        spool.recover_records()?;
        spool.refresh_budget()?;
        for entry in std::fs::read_dir(fd_path(&spool.directory))? {
            let name = entry?.file_name().into_string().map_err(|_| invalid("manager_spool_filename"))?;
            if name.ends_with(".json") {
                let bytes = read_record(&spool.directory, &name)?;
                match Stored::decode(&bytes, &spool.instance)? {
                    Stored::Event(_) => {
                        spool.index.insert(name, bytes.len() as u64);
                    }
                    Stored::Reserved(slot) => {
                        spool.reserved.insert(name, slot);
                    }
                }
            }
        }
        Ok(spool)
    }

    fn recover_records(&mut self) -> io::Result<()> {
        // Collect before renaming: mutating a readdir stream can revisit or skip entries.
        let mut names = Vec::new();
        for entry in std::fs::read_dir(fd_path(&self.directory))? {
            if names.len() > self.limits.records {
                return Err(invalid("manager_spool_capacity"));
            }
            names.push(entry?.file_name().into_string().map_err(|_| invalid("manager_spool_filename"))?);
        }
        names.sort_by_key(|name| name.starts_with(".tmp-"));
        for name in names {
            // A conflicting temporary may already have quarantined this target.
            if record_file(&self.directory, &name).is_err_and(|error| error.kind() == io::ErrorKind::NotFound) {
                continue;
            }
            // Never quarantine a symlink, directory, foreign owner or non-private file.
            let length = record_file(&self.directory, &name)?.metadata()?.len();
            if length == 0 || length > MAX_RECORD as u64 {
                self.quarantine(&name)?;
                continue;
            }
            let bytes = read_record(&self.directory, &name)?;
            let record = match Stored::decode(&bytes, &self.instance) {
                Ok(event) => event,
                Err(_) => {
                    self.quarantine(&name)?;
                    continue;
                }
            };
            let target = record.name();
            if !name.starts_with(".tmp-") {
                if name != target {
                    self.quarantine(&name)?;
                }
                continue;
            }
            match record_file(&self.directory, &target) {
                Ok(_) => {
                    let target_bytes = read_record(&self.directory, &target)?;
                    if target_bytes != bytes {
                        match (Stored::decode(&target_bytes, &self.instance)?, &record) {
                            (Stored::Reserved(slot), Stored::Event(event)) if slot.accepts(event) => {
                                renameat_with(&self.directory, &name, &self.directory, &target, RenameFlags::empty())?;
                                continue;
                            }
                            (Stored::Event(event), Stored::Reserved(slot)) if slot.accepts(&event) => {
                                unlinkat(&self.directory, &name, AtFlags::empty())?;
                                continue;
                            }
                            _ => (),
                        }
                        // Conflicting copies are both evidence, not an invitation to pick one.
                        self.quarantine(&target)?;
                        self.quarantine(&name)?;
                        continue;
                    }
                    unlinkat(&self.directory, &name, AtFlags::empty())?;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    renameat_with(&self.directory, &name, &self.directory, &target, RenameFlags::NOREPLACE)?
                }
                Err(error) => return Err(error),
            }
        }
        self.directory.sync_all()
    }

    fn quarantine(&mut self, name: &str) -> io::Result<()> {
        let length = record_file(&self.directory, name)?.metadata()?.len();
        let directory = child_directory(&self.root, "quarantine")?;
        let (count, bytes) = quarantine_budget(&directory)?;
        if count >= MAX_QUARANTINE_RECORDS || bytes.saturating_add(length) > MAX_QUARANTINE_BYTES {
            return Err(invalid("manager_quarantine_capacity"));
        }
        let destination = format!("{}-{}.bad", self.instance, super::new_id()?);
        renameat_with(&self.directory, name, &directory, &destination, RenameFlags::NOREPLACE)?;
        // A rename never deletes the unacknowledged bytes or frees their global budget.
        directory.sync_all()?;
        self.directory.sync_all()?;
        self.root.sync_all()?;
        self.quarantined += 1;
        Ok(())
    }

    pub fn refresh_budget(&mut self) -> io::Result<()> {
        self.refresh_budget_inner(false)
    }
    fn refresh_budget_inner(&mut self, recovery: bool) -> io::Result<()> {
        let mut count = 0usize;
        let mut bytes = 0u64;
        let mut directories = 0usize;
        let mut quarantine_count = 0;
        for entry in std::fs::read_dir(fd_path(&self.root))? {
            directories += 1;
            if directories > 1024 {
                return Err(invalid("manager_spool_capacity"));
            }
            let name = entry?.file_name().into_string().map_err(|_| invalid("manager_spool_filename"))?;
            if name == "quarantine" {
                let dir: File =
                    openat(&self.root, &name, OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC, Mode::empty())?
                        .into();
                validate_directory(&dir)?;
                let (quarantined, length) = quarantine_budget(&dir)?;
                quarantine_count = quarantined;
                count += quarantined;
                bytes = bytes.checked_add(length).ok_or_else(|| invalid("manager_spool_capacity"))?;
                if count > self.limits.records + usize::from(recovery)
                    || bytes > self.limits.bytes + if recovery { MAX_RECORD as u64 } else { 0 }
                {
                    return Err(invalid("manager_spool_capacity"));
                }
                continue;
            }
            if !canonical_id(&name) {
                return Err(invalid("manager_spool_directory_invalid"));
            }
            let dir: File =
                openat(&self.root, &name, OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC, Mode::empty())?.into();
            validate_directory(&dir)?;
            for item in std::fs::read_dir(fd_path(&dir))? {
                let file = record_file(&dir, item?.file_name())?;
                count += 1;
                bytes = bytes
                    .checked_add(file.metadata()?.len())
                    .ok_or_else(|| invalid("manager_spool_capacity"))?;
                if count > self.limits.records + usize::from(recovery)
                    || bytes > self.limits.bytes + if recovery { MAX_RECORD as u64 } else { 0 }
                {
                    return Err(invalid("manager_spool_capacity"));
                }
            }
        }
        self.total_records = count;
        self.total_bytes = bytes;
        self.quarantined = quarantine_count;
        Ok(())
    }

    pub fn reserve(&mut self, slot: &Reservation) -> io::Result<()> {
        if self.blocked_staging {
            return Err(invalid("manager_spool_recovery_required"));
        }
        slot.validate(&self.instance)?;
        let name = slot.name();
        if self.index.contains_key(&name) {
            let event = match Stored::decode(&read_record(&self.directory, &name)?, &self.instance)? {
                Stored::Event(event) => event,
                _ => return Err(invalid("manager_reservation_conflict")),
            };
            return if slot.accepts(&event) {
                self.directory.sync_all()
            } else {
                Err(invalid("manager_reservation_conflict"))
            };
        }
        if self.reserved.contains_key(&name) {
            return if read_record(&self.directory, &name)? == slot.encode()? {
                self.directory.sync_all()
            } else {
                Err(invalid("manager_reservation_conflict"))
            };
        }
        if self.total_records >= self.limits.records || self.total_bytes + MAX_RECORD as u64 > self.limits.bytes {
            return Err(invalid("manager_spool_capacity"));
        }
        self.commit(&name, &slot.encode()?, false)?;
        self.total_records += 1;
        self.total_bytes += MAX_RECORD as u64;
        self.reserved.insert(name, slot.clone());
        self.directory.sync_all()
    }

    pub fn append(&mut self, event: &Event) -> io::Result<()> {
        self.append_bytes(&event.encode()?)
    }

    pub fn append_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.blocked_staging {
            return Err(invalid("manager_spool_recovery_required"));
        }
        if bytes.len() > MAX_RECORD {
            return Err(invalid("manager_event_too_large"));
        }
        // Also validates maximum depth before the record can enter a durable queue.
        let event: Event = protocol::decode(bytes).map_err(|_| invalid("manager_event_invalid"))?;
        event.validate(&self.instance)?;
        let name = event.name();
        if self.index.contains_key(&name) {
            if read_record(&self.directory, &name)? != bytes {
                return Err(invalid("manager_spool_conflict"));
            }
            return self.directory.sync_all();
        }
        let reserved = if let Some(slot) = self.reserved.get(&name) {
            if !slot.accepts(&event) || read_record(&self.directory, &name)? != slot.encode()? {
                return Err(invalid("manager_reservation_conflict"));
            }
            true
        } else {
            false
        };
        if !reserved && (self.total_records >= self.limits.records || self.total_bytes + bytes.len() as u64 > self.limits.bytes) {
            return Err(invalid("manager_spool_capacity"));
        }
        self.commit(&name, bytes, reserved)?;
        // Account for a committed rename even if the directory sync is ambiguous.
        if reserved {
            self.reserved.remove(&name);
            self.total_bytes -= MAX_RECORD as u64;
        } else {
            self.total_records += 1;
        }
        self.total_bytes += bytes.len() as u64;
        self.index.insert(name, bytes.len() as u64);
        self.directory.sync_all()
    }

    fn commit(&mut self, name: &str, bytes: &[u8], replace: bool) -> io::Result<()> {
        let temp = format!(".tmp-{}", super::new_id()?);
        let result = (|| -> io::Result<()> {
            let mut file: File = openat(
                &self.directory,
                &temp,
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )?
            .into();
            file.write_all(bytes)?;
            file.sync_all()?;
            renameat_with(
                &self.directory,
                &temp,
                &self.directory,
                name,
                if replace { RenameFlags::empty() } else { RenameFlags::NOREPLACE },
            )?;
            Ok(())
        })();
        if result.is_err()
            && let Err(error) = unlinkat(&self.directory, &temp, AtFlags::empty())
            && error != rustix::io::Errno::NOENT
        {
            // Do not accumulate new temporaries on a filesystem that cannot clean up one.
            self.blocked_staging = true;
        }
        result
    }

    pub fn batch(&self) -> io::Result<Vec<Record>> {
        let mut batch = Vec::new();
        let mut total = 0;
        for name in self.index.keys().take(32) {
            let bytes = read_record(&self.directory, name)?;
            if total + bytes.len() > MAX_BATCH - 1024 {
                break;
            }
            let event: Event = protocol::decode(&bytes).map_err(|_| invalid("manager_spool_corrupt"))?;
            event.validate(&self.instance)?;
            if event.name() != *name {
                return Err(invalid("manager_spool_identity_mismatch"));
            }
            total += bytes.len();
            batch.push(Record {
                name: name.clone(),
                bytes,
                event,
            });
        }
        Ok(batch)
    }

    // Called only after a strictly validated HTTP batch ACK. Never accepts an arbitrary path.
    pub fn acknowledge(&mut self, batch: &[Record]) -> io::Result<()> {
        for record in batch {
            if record.event.instance_id != self.instance || record.event.name() != record.name || !self.index.contains_key(&record.name) {
                return Err(invalid("manager_spool_ack_identity"));
            }
            if read_record(&self.directory, &record.name)? != record.bytes {
                return Err(invalid("manager_spool_conflict"));
            }
            unlinkat(&self.directory, &record.name, AtFlags::empty())?;
            if let Some(length) = self.index.remove(&record.name) {
                self.total_records -= 1;
                self.total_bytes -= length;
            }
        }
        self.directory.sync_all()
    }
    pub fn depth(&self) -> usize {
        self.index.len()
    }
    pub fn bytes(&self) -> u64 {
        self.total_bytes
    }
    pub fn quarantined(&self) -> usize {
        self.quarantined
    }
    pub fn write_blocked(&self) -> bool {
        self.blocked_staging
    }
    pub fn next_sequence(&self) -> io::Result<i64> {
        let largest = self.index.keys().chain(self.reserved.keys()).max();
        match largest {
            None => Ok(0),
            Some(name) => name[..20]
                .parse::<i64>()
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| invalid("manager_sequence_exhausted")),
        }
    }
}

// The child owns its lock, including if its Manager parent crashes during filesystem recovery.
pub(super) fn lock_recovery_root(root: &Path) -> io::Result<File> {
    let directory = open_no_follow(root)?;
    validate_directory(&directory)?;
    rustix::fs::flock(&directory, rustix::fs::FlockOperation::NonBlockingLockExclusive)?;
    Ok(directory)
}

fn quarantine_budget(directory: &File) -> io::Result<(usize, u64)> {
    let mut count = 0;
    let mut bytes = 0u64;
    for entry in std::fs::read_dir(fd_path(directory))? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(|| invalid("manager_quarantine_filename"))?;
        if name.len() != 77
            || !name.is_ascii()
            || !canonical_id(&name[..36])
            || name.as_bytes()[36] != b'-'
            || !canonical_id(&name[37..73])
            || !name.ends_with(".bad")
        {
            return Err(invalid("manager_quarantine_filename"));
        }
        let file = record_file(directory, name)?;
        count += 1;
        bytes = bytes
            .checked_add(file.metadata()?.len())
            .ok_or_else(|| invalid("manager_quarantine_capacity"))?;
        if count > MAX_QUARANTINE_RECORDS || bytes > MAX_QUARANTINE_BYTES {
            return Err(invalid("manager_quarantine_capacity"));
        }
    }
    Ok((count, bytes))
}

fn fd_path(file: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
}
fn validate_directory(file: &File) -> io::Result<()> {
    let meta = file.metadata()?;
    if !meta.is_dir() || meta.uid() != rustix::process::geteuid().as_raw() || meta.mode() & 0o7777 != 0o700 {
        return Err(invalid("manager_spool_directory_invalid"));
    }
    Ok(())
}
fn child_directory(parent: &File, name: &str) -> io::Result<File> {
    match mkdirat(parent, name, Mode::RUSR | Mode::WUSR | Mode::XUSR) {
        Ok(()) => (),
        Err(rustix::io::Errno::EXIST) => (),
        Err(error) => return Err(error.into()),
    }
    let file: File = openat(parent, name, OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC, Mode::empty())?.into();
    validate_directory(&file)?;
    Ok(file)
}
fn record_file(dir: &File, name: impl rustix::path::Arg) -> io::Result<File> {
    let file: File = openat(dir, name, OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC, Mode::empty())?.into();
    let meta = file.metadata()?;
    if !meta.is_file() || meta.uid() != rustix::process::geteuid().as_raw() || meta.mode() & 0o7777 != 0o600 {
        return Err(invalid("manager_spool_file_invalid"));
    }
    Ok(file)
}
fn read_record(dir: &File, name: &str) -> io::Result<Vec<u8>> {
    let file = record_file(dir, name)?;
    if file.metadata()?.len() > MAX_RECORD as u64 {
        return Err(invalid("manager_spool_record_size"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_RECORD as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() > MAX_RECORD {
        return Err(invalid("manager_spool_record_size"));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    const INSTANCE: &str = "11111111-1111-4111-8111-111111111111";
    fn private_root() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        root
    }
    fn event(sequence: i64) -> Event {
        Event::new(INSTANCE, sequence, "gallery.download.initialized", serde_json::json!({"gid":123})).unwrap()
    }
    #[test]
    fn restart_preserves_exact_record_and_ack_loss_is_safe() {
        let root = private_root();
        let mut spool = Outbox::open(root.path(), INSTANCE, Limits::default()).unwrap();
        let event = event(1);
        spool.append(&event).unwrap();
        spool.append(&event).unwrap();
        assert_eq!(spool.depth(), 1);
        let original = spool.batch().unwrap().remove(0).bytes;
        drop(spool);
        let mut spool = Outbox::open(root.path(), INSTANCE, Limits::default()).unwrap();
        let records = spool.batch().unwrap();
        assert_eq!(original, records[0].bytes);
        spool.acknowledge(&records).unwrap();
        assert_eq!(spool.depth(), 0);
    }
    #[test]
    fn quota_applies_across_runs_and_never_discards_old_records() {
        let root = private_root();
        let limits = Limits { records: 1, bytes: 16384 };
        let mut old = Outbox::open(root.path(), INSTANCE, limits).unwrap();
        old.append(&event(1)).unwrap();
        drop(old);
        let other = "22222222-2222-4222-8222-222222222222";
        let mut next = Outbox::open(root.path(), other, limits).unwrap();
        let mut new = event(2);
        new.instance_id = other.into();
        assert!(next.append(&new).is_err());
        drop(next);
        let old = Outbox::open(root.path(), INSTANCE, limits).unwrap();
        assert_eq!(old.batch().unwrap().len(), 1);
    }
    #[test]
    fn refuses_links_and_record_substitution() {
        let root = private_root();
        let mut spool = Outbox::open(root.path(), INSTANCE, Limits::default()).unwrap();
        let first = event(1);
        spool.append(&first).unwrap();
        let path = root.path().join(INSTANCE).join(first.name());
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink("/etc/passwd", path).unwrap();
        assert!(spool.batch().is_err());
        assert!(Outbox::open(root.path(), INSTANCE, Limits::default()).is_err());
    }

    #[test]
    fn only_one_writer_and_valid_interrupted_append_recovers_without_new_id() {
        let root = private_root();
        let mut spool = Outbox::open(root.path(), INSTANCE, Limits::default()).unwrap();
        assert!(Outbox::open(root.path(), INSTANCE, Limits::default()).is_err());
        let event = event(1);
        spool.append(&event).unwrap();
        let dir = root.path().join(INSTANCE);
        std::fs::rename(dir.join(event.name()), dir.join(".tmp-interrupted")).unwrap();
        drop(spool);
        let spool = Outbox::open(root.path(), INSTANCE, Limits::default()).unwrap();
        assert_eq!(spool.batch().unwrap()[0].event.event_id, event.event_id);
    }

    #[test]
    fn corrupt_records_are_preserved_in_a_bounded_quarantine_and_still_consume_global_quota() {
        let root = private_root();
        let limits = Limits { records: 2, bytes: 16384 };
        let mut spool = Outbox::open(root.path(), INSTANCE, limits).unwrap();
        let first = event(1);
        spool.append(&first).unwrap();
        let dir = root.path().join(INSTANCE);
        std::fs::write(dir.join(first.name()), b"{\"partial\":").unwrap();
        drop(spool);
        let mut spool = Outbox::open(root.path(), INSTANCE, limits).unwrap();
        assert_eq!(spool.quarantined(), 1);
        assert!(spool.batch().unwrap().is_empty());
        let evidence = std::fs::read_dir(root.path().join("quarantine"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        assert_eq!(std::fs::read(&evidence).unwrap(), b"{\"partial\":");
        spool.append(&event(2)).unwrap();
        assert!(spool.append(&event(3)).is_err());
        drop(spool);
        let mut spool = Outbox::open(root.path(), INSTANCE, limits).unwrap();
        spool.acknowledge(&spool.batch().unwrap()).unwrap();
        assert_eq!(spool.quarantined(), 1);
        assert!(evidence.exists());
    }

    #[test]
    fn conflicting_temporary_and_committed_records_are_both_quarantined() {
        let root = private_root();
        let mut spool = Outbox::open(root.path(), INSTANCE, Limits::default()).unwrap();
        let mut first = event(1);
        spool.append(&first).unwrap();
        first.payload = serde_json::json!({"gid":456});
        let path = root.path().join(INSTANCE).join(".tmp-conflict");
        std::fs::write(&path, first.encode().unwrap()).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        drop(spool);
        let spool = Outbox::open(root.path(), INSTANCE, Limits::default()).unwrap();
        assert_eq!(spool.quarantined(), 2);
        assert!(spool.batch().unwrap().is_empty());
    }

    #[test]
    fn quarantine_exhaustion_fails_closed_without_deleting_the_next_bad_record() {
        let root = private_root();
        let spool = Outbox::open(root.path(), INSTANCE, Limits::default()).unwrap();
        let dir = root.path().join(INSTANCE);
        for index in 0..=MAX_QUARANTINE_RECORDS {
            let path = dir.join(format!(".tmp-broken-{index}"));
            std::fs::write(&path, b"{").unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        drop(spool);
        assert!(Outbox::open(root.path(), INSTANCE, Limits::default()).is_err());
        assert_eq!(std::fs::read_dir(root.path().join("quarantine")).unwrap().count(), MAX_QUARANTINE_RECORDS);
        assert_eq!(std::fs::read_dir(dir).unwrap().count(), 1);
    }

    #[test]
    fn byte_limits_apply_before_append_and_before_quarantining_oversized_corruption() {
        let root = private_root();
        let first = event(1);
        let limits = Limits {
            records: 2,
            bytes: first.encode().unwrap().len() as u64 - 1,
        };
        let mut spool = Outbox::open(root.path(), INSTANCE, limits).unwrap();
        assert!(spool.append(&first).is_err());
        assert_eq!(spool.depth(), 0);
        let path = root.path().join(INSTANCE).join(".tmp-oversized");
        let file = File::create(&path).unwrap();
        file.set_permissions(std::fs::Permissions::from_mode(0o600)).unwrap();
        file.set_len(MAX_QUARANTINE_BYTES + 1).unwrap(); // sparse: no large test allocation
        drop(file);
        drop(spool);
        assert_eq!(Outbox::open(root.path(), INSTANCE, Limits::default()).err().unwrap().to_string(), "manager_quarantine_capacity");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), MAX_QUARANTINE_BYTES + 1);
        assert_eq!(std::fs::read_dir(root.path().join("quarantine")).unwrap().count(), 0);
    }

    #[test]
    fn reserved_terminal_survives_restart_and_can_commit_when_global_queue_is_full() {
        let root = private_root();
        let attempt = super::super::new_id().unwrap();
        let mut terminal = event(2);
        terminal.event_type = "gallery.download.completed".into();
        terminal.payload = serde_json::json!({"eventVersion":2,"downloadAttemptId":attempt});
        let slot = Reservation::new(&terminal, &attempt, true).unwrap();
        let limits = Limits {
            records: 2,
            bytes: MAX_RECORD as u64 * 2,
        };
        let mut spool = Outbox::open(root.path(), INSTANCE, limits).unwrap();
        spool.reserve(&slot).unwrap();
        spool.append(&event(1)).unwrap();
        assert!(spool.append(&event(3)).is_err());
        assert_eq!(spool.batch().unwrap().len(), 1); // placeholders never leave the disk queue
        assert_eq!(spool.next_sequence().unwrap(), 3);
        drop(spool);
        let mut spool = Outbox::open(root.path(), INSTANCE, limits).unwrap();
        let mut wrong = terminal.clone();
        wrong.payload["downloadAttemptId"] = serde_json::json!(super::super::new_id().unwrap());
        assert!(spool.append(&wrong).is_err());
        spool.append(&terminal).unwrap();
        assert_eq!(spool.batch().unwrap().len(), 2);
        assert!(spool.reserved.is_empty());
    }

    #[test]
    fn interrupted_full_queue_replacement_recovers_original_event_instead_of_quarantining_reservation() {
        let root = private_root();
        let attempt = super::super::new_id().unwrap();
        let mut terminal = event(2);
        terminal.event_type = "gallery.download.failed".into();
        terminal.payload = serde_json::json!({"eventVersion":2,"downloadAttemptId":attempt});
        let limits = Limits {
            records: 1,
            bytes: MAX_RECORD as u64,
        };
        let mut spool = Outbox::open(root.path(), INSTANCE, limits).unwrap();
        spool.reserve(&Reservation::new(&terminal, &attempt, true).unwrap()).unwrap();
        let path = root.path().join(INSTANCE).join(".tmp-interrupted-replacement");
        std::fs::write(&path, terminal.encode().unwrap()).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        drop(spool);
        let spool = Outbox::open(root.path(), INSTANCE, limits).unwrap();
        assert_eq!(spool.quarantined(), 0);
        assert!(spool.batch().unwrap()[0].event == terminal);
        assert!(spool.reserved.is_empty());
    }

    #[test]
    fn failed_staging_cleanup_blocks_new_disk_mutations_until_restart_recovery() {
        let root = private_root();
        let mut spool = Outbox::open(root.path(), INSTANCE, Limits::default()).unwrap();
        spool.blocked_staging = true; // Simulate the error branch after an unremovable temporary.
        assert!(spool.write_blocked());
        assert!(spool.append(&event(1)).is_err());
        let slot = Reservation::new(&event(2), &super::super::new_id().unwrap(), true).unwrap();
        assert!(spool.reserve(&slot).is_err());
        assert_eq!(std::fs::read_dir(root.path().join(INSTANCE)).unwrap().count(), 0);
    }
}

use std::{
    collections::HashMap,
    fmt::Display,
    fs::Metadata,
    io::Error,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, SystemTime},
};

use async_stream::stream;
use aws_lc_rs::digest;
use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};
use const_hex::decode_to_array;
use filesize::{file_real_size, file_real_size_fast};
use filetime::{FileTime, set_file_mtime};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt, stream};
use log::{debug, error, info, warn};
use mime::Mime;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tempfile::TempPath;
use tokio::{
    fs::{DirEntry, File, create_dir_all, metadata, read_dir, remove_dir_all, remove_file},
    io::{AsyncReadExt, AsyncWriteExt},
    runtime::{Builder, Handle, Runtime},
    spawn,
    sync::mpsc::{UnboundedSender, channel},
    task::{JoinHandle, block_in_place, spawn_blocking},
    time::{MissedTickBehavior, interval},
};
use tokio_stream::wrappers::ReadDirStream;
use tokio_util::sync::CancellationToken;

use crate::{
    file_reader::FileReader,
    metrics::Metrics,
    rpc::{InitSettings, Settings},
};

const SIZE_100MB: u64 = 100 * 1024 * 1024;

// The staging directory is on the destination filesystem. Readers keep their
// validated temporary inode while the cache becomes visible in one rename.
fn stage_cache_import(source: &Path, destination: &Path, expected: u64) -> Result<(), Error> {
    let parent = destination.parent().ok_or_else(|| Error::other("cache_parent_missing"))?;
    let staging = parent.join(".hath-import");
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(&staging) {
        Ok(()) => (),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(error) => return Err(error),
    }
    let metadata = std::fs::symlink_metadata(&staging)?;
    if !metadata.is_dir() {
        return Err(Error::other("cache_staging_invalid"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        if metadata.uid() != rustix::process::geteuid().as_raw() || metadata.mode() & 0o7777 != 0o700 {
            return Err(Error::other("cache_staging_invalid"));
        }
        let directory = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags((rustix::fs::OFlags::DIRECTORY | rustix::fs::OFlags::NOFOLLOW).bits() as i32)
            .open(&staging)?;
        rustix::fs::flock(&directory, rustix::fs::FlockOperation::LockExclusive)?;
        // Crash leftovers are removed only from this reserved, private directory.
        for entry in std::fs::read_dir(&staging)? {
            let entry = entry?;
            let metadata = std::fs::symlink_metadata(entry.path())?;
            if !entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(".hath-cache-import-"))
                || !metadata.is_file()
                || metadata.uid() != rustix::process::geteuid().as_raw()
                || metadata.nlink() > 2
            {
                return Err(Error::other("cache_staging_unexpected_entry"));
            }
            std::fs::remove_file(entry.path())?;
        }
        stage_cache_file(source, destination, &staging, expected)?;
        directory.sync_all()?;
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        stage_cache_file(source, destination, &staging, expected)
    }
}
fn stage_cache_file(source: &Path, destination: &Path, staging: &Path, expected: u64) -> Result<(), Error> {
    let staged = tempfile::Builder::new()
        .prefix(".hath-cache-import-")
        .tempfile_in(staging)?
        .into_temp_path();
    std::fs::remove_file(&staged)?;
    if std::fs::hard_link(source, &staged).is_err() {
        std::fs::copy(source, &staged)?;
    }
    let file = std::fs::File::open(&staged)?;
    if expected == 0 || file.metadata()?.len() != expected {
        return Err(Error::other("cache_import_length_mismatch"));
    }
    file.sync_all()?;
    let staging_path = staged.to_path_buf();
    staged.persist(destination).map_err(|error| error.error)?;
    // POSIX rename is a no-op when both paths name the same inode (repeat import).
    match std::fs::remove_file(staging_path) {
        Ok(()) => (),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(error) => return Err(error),
    }
    Ok(())
}

pub struct CacheManager {
    mutations: tokio::sync::Mutex<()>,
    jobs: crate::cache_jobs::CacheJobs,
    cache_dir: PathBuf,
    cache_state: Mutex<HashMap<String, CacheState>>,
    cache_state_path: PathBuf,
    temp_dir: PathBuf,
    size_limit: AtomicU64,
    io_pool: Option<Runtime>,
    metrics: Arc<Metrics>,
    background: Mutex<Option<JoinHandle<()>>>,
    background_stop: CancellationToken,
    scan_complete: AtomicBool,
}

pub struct CacheConfig {
    data_dir: PathBuf,
    cache_dir: PathBuf,
    temp_dir: PathBuf,
}

/// Result of a cache file read operation
pub enum CacheFileResult<S> {
    /// Full file content from the beginning
    Full(S),
    /// Partial content from offset
    Partial(S),
    /// File not found or error
    NotFound,
}

#[derive(Serialize, Deserialize)]
struct CacheState {
    file_count: u64,
    size: u64,
    oldest: i64,
}

impl CacheConfig {
    pub fn new<P: AsRef<Path>>(data_dir: P, cache_dir: P, temp_dir: P) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            cache_dir: cache_dir.as_ref().to_path_buf(),
            temp_dir: temp_dir.as_ref().to_path_buf(),
        }
    }
}

impl CacheState {
    fn add_file(&mut self, size: u64) {
        self.file_count = self.file_count.saturating_add(1);
        self.size = self.size.saturating_add(size);
    }

    fn remove_file(&mut self, size: u64) -> (u64, u64) {
        let count = u64::from(self.file_count > 0);
        let bytes = self.size.min(size);
        self.file_count = self.file_count.saturating_sub(1);
        self.size = self.size.saturating_sub(size);
        (count, bytes)
    }
}

impl Default for CacheState {
    fn default() -> Self {
        Self {
            file_count: 0,
            size: 0,
            oldest: FileTime::now().unix_seconds(),
        }
    }
}

impl Drop for CacheManager {
    fn drop(&mut self) {
        // IO Pool need shutdown in blocking context
        if let Some(io_pool) = self.io_pool.take() {
            block_in_place(|| io_pool.shutdown_background());
        }
    }
}

impl CacheManager {
    pub async fn new(
        config: CacheConfig,
        settings: Arc<Settings>,
        init_settings: &InitSettings,
        force_background_scan: bool,
        shutdown: UnboundedSender<()>,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Self>, Error> {
        let io_pool = Builder::new_multi_thread().thread_name("IO-Worker").worker_threads(20).build()?;
        let new = Arc::new(Self {
            mutations: tokio::sync::Mutex::new(()),
            jobs: crate::cache_jobs::CacheJobs::new(),
            cache_dir: config.cache_dir.clone(),
            cache_state: Default::default(),
            cache_state_path: config.data_dir.join("cache_state.dat"),
            temp_dir: config.temp_dir.clone(),
            size_limit: AtomicU64::new(u64::MAX),
            io_pool: Some(io_pool),
            metrics,
            background: Mutex::new(None),
            background_stop: CancellationToken::new(),
            scan_complete: AtomicBool::new(false),
        });
        new.update_settings(settings);

        delete_java_cache_data(&config.data_dir).await; // Force official version rescan cache
        clean_temp_dir(&config.temp_dir).await;

        let manager = new.clone();
        let verify_cache = init_settings.verify_cache();
        let static_range = init_settings.static_range();
        let free = get_available_space(&config.cache_dir);
        let low_disk = free.is_some_and(|x| x < SIZE_100MB);

        // Load cache state
        if !verify_cache {
            let old_state = match read_state(&new.cache_state_path).await {
                Ok(state) => state,
                Err(err) => {
                    error!("Load cache state error: err={}", err);
                    HashMap::new()
                }
            };
            for (sr, state) in old_state {
                if static_range.contains(&sr) {
                    new.metrics.static_range.inc();
                    new.metrics.cache_count.inc_by(state.file_count);
                    new.metrics.cache_size.inc_by(state.size);

                    new.cache_state.lock().insert(sr, state);
                }
            }
        }

        if low_disk || (verify_cache && !force_background_scan) {
            // Low space or force cache check
            if low_disk {
                warn!("Disk space is low than 100MiB: available={}MiB", free.unwrap() / 1024 / 1024);
            }

            if verify_cache {
                info!("Start force cache check");
            } else {
                info!("Start foreground cache scan due to low disk space");
            }
            new.scan_cache(static_range, 16, verify_cache).await?;
            new.scan_complete.store(true, Relaxed);
            CacheManager::start_background_task(manager, None);
        } else {
            // Background cache scan
            if verify_cache {
                info!("Start background force cache check");
            } else {
                info!("Start background cache scan");
            }
            CacheManager::start_background_task(manager, Some((static_range, verify_cache, shutdown)));
        }

        Ok(new)
    }

    pub async fn create_temp_file(&self) -> Result<TempPath, Error> {
        let temp_dir = self.temp_dir.clone();
        spawn_blocking(|| {
            tempfile::Builder::new()
                .prefix("proxyfile_")
                .tempfile_in(temp_dir)
                .map(|file| file.into_temp_path())
        })
        .await
        .map_err(|_| Error::other("cache_temp_worker_failed"))?
    }

    pub async fn get_file(
        self: &Arc<Self>,
        info: &CacheFileInfo,
        offset: Option<u64>,
    ) -> CacheFileResult<impl Stream<Item = Result<Bytes, Error>> + use<>> {
        if self.jobs.stopped() {
            return CacheFileResult::NotFound;
        }
        let path = info.to_path(&self.cache_dir);

        // Check exists and open file
        let io_pool = self.get_io_pool();
        let path2 = path.clone();
        let Ok(Ok(metadata)) = io_pool.spawn(async move { std::fs::metadata(&path2) }).await else {
            return CacheFileResult::NotFound;
        };
        if !metadata.is_file() || metadata.len() != info.size() as u64 {
            warn!("Unexpected cache file metadata: path={:?} type={:?}, size={}", path, metadata.file_type(), metadata.len());
            return CacheFileResult::NotFound;
        }
        let path2 = path.clone();
        let mut file_reader = match io_pool.spawn(async move { std::fs::File::open(&path2) }).await {
            Ok(Ok(file)) => FileReader::new(io_pool.clone(), file, 64 * 1024), // 64KiB buffer
            Ok(Err(err)) => {
                error!("Open cache file error: path={:?}, err={}", path, err);
                return CacheFileResult::NotFound;
            }
            Err(err) => {
                error!("Open cache file error: path={:?}, err={}", path, err);
                return CacheFileResult::NotFound;
            }
        };

        // Skip hash check if file is recently accessed
        let one_week_ago = SystemTime::now() - Duration::from_secs(60 * 60 * 24 * 7);
        if FileTime::from_last_modification_time(&metadata) >= one_week_ago.into() {
            // Handle offset for range request
            if let Some(offset) = offset
                && offset != 0
                && file_reader.seek(offset).await.is_ok()
            {
                return CacheFileResult::Partial(file_reader.boxed());
            }
            return CacheFileResult::Full(file_reader.boxed());
        }

        let cache_manager = self.clone();
        let info = info.clone();
        let (tx, mut rx) = channel::<Result<Bytes, Error>>(1);
        let stop = self.jobs.token();
        if !self.jobs.spawn(async move {
            let file_size = metadata.len();
            let mut read_off = 0;
            let mut hasher = Some(digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY));

            while read_off < file_size {
                if tx.is_closed()||stop.is_cancelled(){return;}
                let buffer = match tokio::select!{biased; _=stop.cancelled()=>return,result=file_reader.read()=>result} {
                    Ok(buf) => buf,
                    Err(err) => {
                        error!("Read cache file error: path={:?}, err={}", path, err);
                        tokio::select!{biased;_=stop.cancelled()=>(),_=tx.send(Err(err))=>()}
                        return;
                    }
                };
                if buffer.is_empty()||read_off.saturating_add(buffer.len() as u64)>file_size {
                    tokio::select!{biased;_=stop.cancelled()=>(),_=tx.send(Err(Error::new(std::io::ErrorKind::UnexpectedEof,"cache_view_length_mismatch")))=>()}return;
                }
                read_off += buffer.len() as u64;
                hasher.as_mut().unwrap().update(&buffer);
                let last=read_off==file_size;
                if last {
                    let hash=hasher.take().unwrap().finish();
                    if hash.as_ref()!=info.hash(){
                        tokio::select!{biased;_=stop.cancelled()=>(),_=tx.send(Err(Error::new(std::io::ErrorKind::InvalidData,"cache_hash_mismatch")))=>()}
                        cache_manager.remove_cache(&info).await;return;
                    }
                    // A cancelled Range/consumer must not grant a week of trust
                    // to a file whose full integrity check never finished.
                    cache_manager.mark_recently_accessed(&info).await;
                }
                if let Some(offset) = offset {
                    // Skip buffer out of request range
                    if offset < read_off {
                        let mut buffer = buffer;
                        // Cut buffer to offset
                        if offset > read_off - buffer.len() as u64 {
                            let cut = buffer.len() - (read_off - offset) as usize;
                            buffer = buffer.split_off(cut)
                        }

                        if tokio::select!{biased; _=stop.cancelled()=>true,result=tx.send(Ok(buffer))=>result.is_err()}{return;}
                    }
                } else {
                    if tokio::select!{biased; _=stop.cancelled()=>true,result=tx.send(Ok(buffer))=>result.is_err()}{return;}
                }
                if last{return;}
            }

        }) {return CacheFileResult::NotFound;}

        let stream = stream! {
            while let Some(item) = rx.recv().await {
                yield item;
            }
        }
        .boxed();
        if offset.is_some() {
            CacheFileResult::Partial(stream)
        } else {
            CacheFileResult::Full(stream)
        }
    }

    pub async fn import_cache(&self, info: &CacheFileInfo, file_path: &TempPath) -> bool {
        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
        let mut observation = self
            .metrics
            .observation
            .operation(crate::manager_integration::Operation::CacheImport);
        let success = self.import_cache_inner(info, file_path).await;
        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
        observation.result(success);
        success
    }
    async fn import_cache_inner(&self, info: &CacheFileInfo, file_path: &TempPath) -> bool {
        let _mutation = self.mutations.lock().await;
        let path = info.to_path(&self.cache_dir);
        let dir = path.parent().unwrap();

        if metadata(dir).await.is_err()
            && let Err(err) = create_dir_all(dir).await
        {
            error!("Create cache directory fail: {}", err);
            return false;
        }

        let old_size = async_filesize(&path).await;
        {
            let source = file_path.to_path_buf();
            let destination = path.clone();
            let expected = info.size() as u64;
            let result = spawn_blocking(move || stage_cache_import(&source, &destination, expected)).await;
            if !matches!(result, Ok(Ok(()))) {
                crate::logger::event(crate::logger::Diagnostic::CacheImportFailed);
                return false;
            }
        }

        // Fix permission
        fix_permission(&path).await;

        if let Some(size) = async_filesize(&path).await {
            if let Some(old_size) = old_size {
                self.remove_accounted_size(info, old_size);
            }
            self.cache_state.lock().entry(info.static_range()).or_default().add_file(size);

            self.metrics.cache_count.inc();
            self.metrics.cache_size.inc_by(size);
        }
        true
    }

    pub async fn remove_cache(&self, info: &CacheFileInfo) -> Option<()> {
        let _mutation = self.mutations.lock().await;
        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
        let mut observation = self
            .metrics
            .observation
            .operation(crate::manager_integration::Operation::CacheDelete);
        let path = info.to_path(&self.cache_dir);
        let metadata = match metadata(&path).await {
            Ok(value) => value,
            Err(error) => {
                #[cfg(all(feature = "manager-integration", target_os = "linux"))]
                observation.finish(if error.kind() == std::io::ErrorKind::NotFound {
                    crate::manager_integration::Outcome::Noop
                } else {
                    crate::manager_integration::Outcome::Failed
                });
                #[cfg(not(all(feature = "manager-integration", target_os = "linux")))]
                let _ = error;
                return None;
            }
        };
        let Some(size) = async_filesize_fast(&path, &metadata).await else {
            #[cfg(all(feature = "manager-integration", target_os = "linux"))]
            observation.result(false);
            return None;
        };

        debug!("Delete cache: {:?}", path);
        if let Err(err) = std::fs::remove_file(&path) {
            error!("Delete cache file error: path={:?}, err={}", path, err);
            #[cfg(all(feature = "manager-integration", target_os = "linux"))]
            observation.result(false);
            return None;
        }

        self.remove_accounted_size(info, size);

        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
        observation.result(true);
        Some(())
    }

    // An existing inode is not proof that this aggregate already accounted it.
    // Recovery/scan windows must not turn an unsigned gauge into u64::MAX.
    fn remove_accounted_size(&self, info: &CacheFileInfo, size: u64) {
        if let Some(state) = self.cache_state.lock().get_mut(&info.static_range()) {
            let (count, bytes) = state.remove_file(size);
            let _ = self
                .metrics
                .cache_count
                .inner()
                .try_update(Relaxed, Relaxed, |value| Some(value.saturating_sub(count)));
            let _ = self
                .metrics
                .cache_size
                .inner()
                .try_update(Relaxed, Relaxed, |value| Some(value.saturating_sub(bytes)));
        }
    }

    pub fn update_settings(&self, settings: Arc<Settings>) {
        let size_limit = settings.size_limit();
        info!("Set size limit to {:} GiB", size_limit / 1024 / 1024 / 1024);
        self.size_limit.store(size_limit, Relaxed);
        self.metrics.cache_capacity.set(size_limit.try_into().unwrap_or(i64::MAX));
        self.metrics.cache_configured.store(true, Relaxed);
    }

    pub async fn save_state(&self) {
        if !self.scan_complete.load(Relaxed) {
            // Cancellation or a scan failure cannot replace the last saved state
            // with a partially rebuilt map.
            return;
        }
        let mut file = match File::create(&self.cache_state_path).await {
            Ok(file) => file,
            Err(err) => {
                error!("Create cache state file error: err={}", err);
                return;
            }
        };
        let buf = match rmp_serde::to_vec_named(&*self.cache_state.lock()) {
            Ok(buf) => buf,
            Err(err) => {
                error!("Serialize cache state error: err={}", err);
                return;
            }
        };
        if let Err(err) = file.write_all(&buf).await {
            error!("Write cache state error: err={}", err);
        }
    }

    fn get_io_pool(&self) -> Handle {
        self.io_pool.as_ref().map_or_else(|| Handle::current(), |rt| rt.handle().clone())
    }

    pub async fn shutdown_background(&self) {
        self.stop_io();
        self.jobs.wait().await;
        self.background_stop.cancel();
        let task = self.background.lock().take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
    pub(crate) fn spawn_io(&self, future: impl std::future::Future<Output = ()> + Send + 'static) -> bool {
        self.jobs.spawn(future)
    }
    pub(crate) fn io_stop(&self) -> tokio_util::sync::CancellationToken {
        self.jobs.token()
    }
    pub(crate) fn io_stopped(&self) -> bool {
        self.jobs.stopped()
    }
    pub(crate) fn stop_io(&self) {
        self.background_stop.cancel();
        self.jobs.close();
    }

    fn start_background_task(new: Arc<Self>, scan: Option<(Vec<String>, bool, UnboundedSender<()>)>) {
        let manager = Arc::downgrade(&new);
        let stop = new.background_stop.clone();
        let task = spawn(async move {
            if let Some((ranges, verify, shutdown)) = scan {
                let Some(manager) = manager.upgrade() else { return };
                let result =
                    tokio::select! { biased; _ = stop.cancelled() => return, result = manager.scan_cache(ranges, 4, verify) => result };
                if let Err(err) = result {
                    error!("Cache scan error: {}", err);
                    let _ = shutdown.send(());
                    return;
                }
                manager.scan_complete.store(true, Relaxed);
            }
            // Check cache size every 10min
            let mut interval = interval(Duration::from_mins(10));
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! { biased; _ = stop.cancelled() => break, _ = interval.tick() => () }
                if let Some(manager) = manager.upgrade() {
                    manager.save_state().await;
                    if !stop.is_cancelled() {
                        manager.check_cache_usage().await;
                    }
                } else {
                    break;
                }
            }
        });
        *new.background.lock() = Some(task);
    }

    async fn mark_recently_accessed(&self, info: &CacheFileInfo) {
        let path = info.to_path(&self.cache_dir);
        let _ = spawn_blocking(move || {
            if let Err(err) = set_file_mtime(&path, FileTime::now()) {
                error!("Update cache file time error: path={:?}, err={}", path, err);
            }
        })
        .await;
    }

    async fn scan_cache(&self, static_range: Vec<String>, parallelism: usize, verify_cache: bool) -> Result<(), Error> {
        self.metrics.cache_scan_complete.store(false, Relaxed);
        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
        let mut observation = self.metrics.observation.operation(crate::manager_integration::Operation::CacheScan);
        let result = self.scan_cache_inner(static_range, parallelism, verify_cache).await;
        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
        observation.result(result.is_ok());
        self.metrics.cache_scan_complete.store(result.is_ok(), Relaxed);
        result
    }
    async fn scan_cache_inner(&self, static_range: Vec<String>, parallelism: usize, verify_cache: bool) -> Result<(), Error> {
        let mut dirs = Vec::with_capacity(static_range.len());

        let mut root = read_dir(&self.cache_dir).await?;
        while let Some(l1) = root.next_entry().await? {
            let l1_path = l1.path();
            if !l1_path.is_dir() {
                warn!("Found unexpected file in cache dir: {}", l1_path.to_str().unwrap_or_default());
                continue;
            };

            let mut l1_stream = read_dir(&l1_path).await?;
            while let Some(l2) = l1_stream.next_entry().await? {
                let l2_path = l2.path();
                if !l2_path.is_dir() {
                    warn!("Found unexpected file in cache dir: {}", l2_path.to_str().unwrap_or_default());
                    continue;
                };

                let mut hash = l1.file_name().clone();
                hash.push(l2.file_name());
                if let Some(sr) = static_range.iter().find(|sr| hash.eq_ignore_ascii_case(sr)) {
                    dirs.push((sr, l2_path));
                } else {
                    warn!("Delete not in static range dir: {}", l2_path.to_str().unwrap_or_default());
                    if let Err(err) = remove_dir_all(&l2_path).await {
                        error!("Delete cache dir error: path={:?}, err={}", l2_path, err);
                    }
                }
            }
        }

        debug!("Cache dir number: {}", dirs.len());

        let mut counter = 0;
        let total = dirs.len();
        for (sr, path) in dirs.into_iter() {
            // Check old state match
            let file_count = match open_dir(&path).await {
                Some(stream) => {
                    stream
                        .filter_map(|e| async { e.ok().filter(|entry| entry.file_name() != ".hath-import") })
                        .count()
                        .await as u64
                }
                None => continue,
            };
            if file_count == self.cache_state.lock().get(sr).map(|s| s.file_count).unwrap_or(0) {
                counter += 1;
                if counter % 100 == 0 || counter == total {
                    info!("Scanned {}/{} static ranges.", counter, total);
                }
                continue;
            }

            // State mismatch, scan dir
            self.cache_state.lock().remove(sr); // Clear old state

            let files = match open_dir(&path).await {
                Some(mut stream) => list_files(&mut stream).await,
                None => continue,
            };

            stream::iter(files)
                .for_each_concurrent(parallelism, |entry| async move {
                    let path = entry.path();
                    let metadata = entry.metadata().await;
                    if let Err(err) = metadata {
                        error!("Read cache file metadata error: path={:?}, err={}", path, err);
                        return;
                    }
                    let metadata = metadata.unwrap();
                    if metadata.is_dir() {
                        if entry.file_name() == ".hath-import" {
                            return;
                        }
                        warn!("Found unexpected dir in cache dir: {:?}", path);
                        return;
                    }

                    // Parse info
                    let info = entry.file_name().to_str().and_then(CacheFileInfo::from_file_id);
                    if info.is_none() {
                        warn!("Invalid cache file: {:?}", path);
                        return;
                    }
                    let info = info.unwrap();

                    let size = metadata.len();
                    if size != info.size() as u64 {
                        warn!("Delete corrupt cache file: path={:?}, size={:x?}, actual={:x?}", path, info.size(), size);

                        if let Err(err) = remove_file(&path).await {
                            error!("Delete corrupt cache file error: path={:?}, err={}", path, err);
                        }
                        return;
                    }

                    if verify_cache {
                        let mut file = match File::open(&path).await {
                            Ok(file) => file,
                            Err(err) => {
                                error!("Open cache file {:?} error: {}", path, err);
                                return;
                            }
                        };
                        let mut hasher = digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY);
                        let mut buf = vec![0; 1024 * 1024]; // 1MiB
                        loop {
                            match file.read(&mut buf).await {
                                Ok(n) => {
                                    if n == 0 {
                                        break;
                                    }
                                    hasher.update(&buf[0..n]);
                                }
                                Err(e) => {
                                    error!("Read cache file {:?} error: {}", path, e);
                                    break;
                                }
                            }
                        }
                        let actual_hash = hasher.finish();
                        if actual_hash.as_ref() != info.hash {
                            warn!("Delete corrupt cache file: path={:?}, hash={:x?}, actual={:x?}", path, info.hash, actual_hash);
                            if let Err(err) = remove_file(&path).await {
                                error!("Delete corrupt cache file error: path={:?}, err={}", path, err);
                            }
                            return;
                        }
                    }

                    // Update cache state
                    if let Some(size) = async_filesize_fast(&path, &metadata).await {
                        let mtime = FileTime::from_last_modification_time(&metadata).unix_seconds();
                        let mut cache_state = self.cache_state.lock();
                        let state = cache_state.entry(info.static_range()).or_default();
                        state.add_file(size);
                        if state.oldest > mtime {
                            state.oldest = mtime;
                        }
                    }
                })
                .await;

            // Scan progress
            counter += 1;
            if counter % 100 == 0 || counter == total {
                info!("Scanned {}/{} static ranges.", counter, total);
            }
        }

        if counter == 0 && static_range.len() > 20 {
            error!(
                "This client has static ranges assigned to it, but the cache is empty. Check file permissions and file system integrity."
            );
            return Err(Error::new(std::io::ErrorKind::NotFound, "Cache is empty."));
        }

        let (total_size, file_count) = self
            .cache_state
            .lock()
            .values()
            .fold((0, 0), |(size, count), state| (size + state.size, count + state.file_count));
        info!("Finished cache scan. Cache size: {}, file count: {}", total_size, file_count);

        self.metrics.cache_size.set(total_size);
        self.metrics.cache_count.set(file_count);
        self.metrics.static_range.set(self.cache_state.lock().len() as i64);

        Ok(())
    }

    async fn check_cache_usage(&self) {
        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
        let mut observation = self
            .metrics
            .observation
            .operation(crate::manager_integration::Operation::CachePrune);
        self.check_cache_usage_inner().await;
        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
        observation.result(true);
    }
    async fn check_cache_usage_inner(&self) {
        let total_size = self.cache_state.lock().values().fold(0, |acc, state| acc + state.size);
        let size_limit = self.size_limit.load(Relaxed);
        let disk_free = get_available_space(&self.cache_dir);
        let mut need_free = total_size.saturating_sub(size_limit);

        // Update metrics
        self.metrics.cache_size.set(total_size);
        self.metrics.static_range.set(self.cache_state.lock().len() as i64);

        if let Some(free) = disk_free
            && free < SIZE_100MB
        {
            warn!("Disk space is low than 100MiB: available={}MiB", free / 1024 / 1024);
            need_free += SIZE_100MB.saturating_sub(free);
        }

        debug!("Cache usage: totel={total_size}bytes, limit={size_limit}bytes, available={disk_free:?}bytes");

        if need_free == 0 {
            return;
        }

        // Minimum cleanup 100MB
        if need_free < SIZE_100MB {
            need_free = SIZE_100MB;
        }

        debug!("Start cache cleaner: need_free={}bytes", need_free);
        while need_free > 0 && size_limit == self.size_limit.load(Relaxed) {
            // Select the oldest directory
            let static_range;
            let target_dir;
            let cut_off;
            {
                let map = self.cache_state.lock();
                let mut state = map.iter().collect::<Vec<_>>();
                if state.is_empty() {
                    return;
                }
                state.sort_unstable_by_key(|(_, a)| a.oldest);
                let (sr, state) = state[0];
                static_range = sr.clone();
                target_dir = self.cache_dir.join(&sr[0..2]).join(&sr[2..4]);
                // Set max cleanup cutoff based on the oldest time
                let offset = match DateTime::from_timestamp_secs(state.oldest).map(|date| Utc::now().signed_duration_since(date)) {
                    Some(date) if date > TimeDelta::days(180) => 86400 * 30, // 30 days
                    Some(date) if date > TimeDelta::days(90) => 86400 * 7,   // 7 days
                    Some(date) if date > TimeDelta::days(30) => 86400 * 3,   // 3 days
                    _ => 86400,                                              // Default 1 day
                };
                cut_off = FileTime::from_unix_time(state.oldest + offset, 0);
            }

            // List files
            let files = match open_dir(&target_dir).await {
                Some(mut stream) => list_files(&mut stream).await,
                None => break,
            };

            // Sort by mtime
            let mut files: Vec<(DirEntry, FileTime, Metadata)> = stream::iter(files)
                .filter_map(|entry| async move {
                    let metadata = match entry.metadata().await {
                        Ok(metadata) => metadata,
                        Err(err) => {
                            error!("Read cache dir {:?} error: {}", entry.path(), err);
                            return None;
                        }
                    };

                    if metadata.is_file() {
                        Some((entry, FileTime::from_last_modification_time(&metadata), metadata))
                    } else {
                        None
                    }
                })
                .collect()
                .await;
            files.sort_unstable_by_key(|(_, a, _)| *a);

            // Delete cache until need_free is 0 or mtime is new than cut_off
            let mut new_oldest = files.last().map_or_else(FileTime::now, |(_, mtime, _)| *mtime);
            for (entry, mtime, metadata) in files {
                if mtime > cut_off || need_free == 0 {
                    new_oldest = mtime;
                    break;
                }

                let info = entry.file_name().to_str().and_then(CacheFileInfo::from_file_id);
                let path = entry.path();
                if info.is_none() {
                    warn!("Invalid cache file: {:?}", path);
                    continue;
                }

                if let Some(size) = async_filesize_fast(&path, &metadata).await {
                    self.remove_cache(&info.unwrap()).await;
                    need_free = need_free.saturating_sub(size);
                }
            }

            // Update oldest
            if let Some(state) = self.cache_state.lock().get_mut(&static_range) {
                state.oldest = new_oldest.unix_seconds();
            }
            self.save_state().await;
        }
    }
}

async fn async_filesize(path: &Path) -> Option<u64> {
    let path2 = path.to_path_buf();
    match spawn_blocking(move || file_real_size(path2)).await {
        Ok(Ok(size)) => Some(size),
        Ok(Err(e)) => {
            error!("Read cache file {:?} size error: {}", path, e);
            None
        }
        Err(e) => {
            error!("Read cache file {:?} size error: {}", path, e);
            None
        }
    }
}

async fn async_filesize_fast(path: &Path, metadata: &Metadata) -> Option<u64> {
    let path2 = path.to_path_buf();
    let metadata = metadata.clone();
    match spawn_blocking(move || file_real_size_fast(path2, &metadata)).await {
        Ok(Ok(size)) => Some(size),
        Ok(Err(e)) => {
            error!("Read cache file {:?} size error: {}", path, e);
            None
        }
        Err(e) => {
            error!("Read cache file {:?} size error: {}", path, e);
            None
        }
    }
}

async fn delete_java_cache_data<P: AsRef<Path>>(data_dir: P) {
    let base = data_dir.as_ref();
    let _ = remove_file(base.join("pcache_info")).await;
    let _ = remove_file(base.join("pcache_ages")).await;
    let _ = remove_file(base.join("pcache_lru")).await;
}

async fn read_state(path: &Path) -> Result<HashMap<String, CacheState>, Box<dyn std::error::Error>> {
    let mut file = File::open(path).await?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await?;
    Ok(rmp_serde::from_slice(&buf)?)
}

async fn open_dir(path: &Path) -> Option<ReadDirStream> {
    read_dir(path)
        .map_ok(ReadDirStream::new)
        .await
        .inspect_err(|err| error!("Read cache dir error: path={:?}, err={}", path, err))
        .ok()
}

async fn list_files(stream: &mut ReadDirStream) -> Vec<DirEntry> {
    let mut files: Vec<DirEntry> = stream
        .inspect_err(|err| error!("List cache file error: {}", err))
        .filter_map(|entry| async { entry.ok() })
        .collect()
        .await;

    sort_by_inode(&mut files);

    files
}

#[cfg(unix)]
fn sort_by_inode(files: &mut [DirEntry]) {
    files.sort_by_key(|f| f.ino());
}

#[cfg(not(unix))]
fn sort_by_inode(_files: &mut [DirEntry]) {
    // Not support
}

#[cfg(unix)]
async fn fix_permission(path: &Path) {
    use std::os::unix::prelude::PermissionsExt;

    use tokio::fs::set_permissions;

    _ = set_permissions(&path, PermissionsExt::from_mode(0o644)).await;
}

#[cfg(not(unix))]
async fn fix_permission(_path: &Path) {
    // Skip
}

#[cfg(unix)]
fn get_available_space(path: &Path) -> Option<u64> {
    use rustix::fs::statvfs;
    if let Ok(stat) = statvfs(path) {
        Some(stat.f_bavail * stat.f_bsize)
    } else {
        None
    }
}

#[cfg(windows)]
fn get_available_space(path: &Path) -> Option<u64> {
    use windows::{Win32::Storage::FileSystem::GetDiskFreeSpaceExW, core::HSTRING};

    let full_path = path.canonicalize().ok()?;
    let mut free: u64 = 0;

    if unsafe { GetDiskFreeSpaceExW(&HSTRING::from(full_path.as_path()), Some(&mut free), None, None) }.is_ok() {
        Some(free)
    } else {
        None
    }
}

#[cfg(not(any(unix, windows)))]
fn get_available_space(_path: &Path) -> Option<u64> {
    None // Not support
}

async fn clean_temp_dir(path: &Path) {
    info!("Deleting old temp files");

    if let Ok(mut dir) = read_dir(path).await {
        while let Ok(Some(file)) = dir.next_entry().await {
            let path = file.path();
            if path.is_file() && file.file_name().to_string_lossy().starts_with("proxyfile_") {
                debug!("Delete old temp file: {:?}", path);
                let _ = remove_file(path).await;
            }
        }
    }
}

#[derive(Clone, Hash, Eq, PartialEq)]
enum FileType {
    Jpeg,
    Png,
    Gif,
    Mp4,
    Webm,
    Webp,
    Avif,
    Jpegxl,
    Other(String),
}

impl FileType {
    fn to_mime(&self) -> Mime {
        match self {
            Self::Jpeg => mime::IMAGE_JPEG,
            Self::Png => mime::IMAGE_PNG,
            Self::Gif => mime::IMAGE_GIF,
            Self::Mp4 => "video/mp4".parse().unwrap(),
            Self::Webm => "video/webm".parse().unwrap(),
            Self::Webp => "image/webp".parse().unwrap(),
            Self::Avif => "image/avif".parse().unwrap(),
            Self::Jpegxl => "image/jxl".parse().unwrap(),
            Self::Other(_) => mime::APPLICATION_OCTET_STREAM,
        }
    }

    fn as_str(&self) -> &str {
        match self {
            Self::Jpeg => "jpg",
            Self::Png => "png",
            Self::Gif => "gif",
            Self::Mp4 => "mp4",
            Self::Webm => "wbm",
            Self::Webp => "wbp",
            Self::Avif => "avf",
            Self::Jpegxl => "jxl",
            Self::Other(s) => s,
        }
    }
}

impl Display for FileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for FileType {
    fn from(s: &str) -> Self {
        match s {
            "jpg" => Self::Jpeg,
            "png" => Self::Png,
            "gif" => Self::Gif,
            "mp4" => Self::Mp4,
            "wbm" => Self::Webm,
            "wbp" => Self::Webp,
            "avf" => Self::Avif,
            "jxl" => Self::Jpegxl,
            _ => Self::Other(s.to_string()),
        }
    }
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub struct CacheFileInfo {
    hash: [u8; 20],
    size: u32,
    xres: u32,
    yres: u32,
    mime_type: FileType,
}

impl CacheFileInfo {
    pub fn from_file_id<T: AsRef<str>>(id: T) -> Option<CacheFileInfo> {
        let mut part = id.as_ref().split('-');
        let none_res = part.clone().count() == 3;

        Some(CacheFileInfo {
            hash: decode_to_array(part.next()?).ok()?,
            size: part.next().and_then(|s| s.parse().ok())?,
            xres: if none_res { 0 } else { part.next().and_then(|s| s.parse().ok())? },
            yres: if none_res { 0 } else { part.next().and_then(|s| s.parse().ok())? },
            mime_type: part.next()?.into(),
        })
    }

    /// Get the cache file's size.
    pub fn size(&self) -> u32 {
        self.size
    }

    pub fn hash(&self) -> [u8; 20] {
        self.hash
    }

    fn to_path(&self, cache_dir: &Path) -> PathBuf {
        let hash = const_hex::encode(self.hash);
        let filename = if self.xres > 0 {
            format!("{}-{}-{}-{}-{}", hash, self.size, self.xres, self.yres, self.mime_type)
        } else {
            format!("{}-{}-{}", hash, self.size, self.mime_type)
        };
        let base = cache_dir.as_os_str();
        let mut path = PathBuf::with_capacity(base.len() + 7 + filename.len()); // base + 2 level dir + filename length
        path.push(base);
        path.push(&hash[0..2]);
        path.push(&hash[2..4]);
        path.push(filename);
        path
    }

    fn static_range(&self) -> String {
        const_hex::encode(&self.hash[0..2])
    }

    pub fn mime_type(&self) -> Mime {
        self.mime_type.to_mime()
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    #[tokio::test]
    async fn atomic_import_preserves_live_source_and_replacement_accounting() {
        let root = tempfile::tempdir().unwrap();
        let manager = manager(root.path());
        let bytes = b"immutable-cache";
        let hash = const_hex::encode(digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, bytes));
        let info = CacheFileInfo::from_file_id(format!("{hash}-{}-jpg", bytes.len())).unwrap();
        let source = manager.create_temp_file().await.unwrap();
        std::fs::write(&source, bytes).unwrap();
        assert!(manager.import_cache(&info, &source).await);
        assert!(manager.import_cache(&info, &source).await);
        assert_eq!(std::fs::read(&source).unwrap(), bytes);
        assert_eq!(std::fs::read(info.to_path(root.path())).unwrap(), bytes);
        assert_eq!(manager.metrics.cache_count.get(), 1);
        let staging = info.to_path(root.path()).parent().unwrap().join(".hath-import");
        assert_eq!(std::fs::read_dir(staging).unwrap().count(), 0);
        let short = manager.create_temp_file().await.unwrap();
        std::fs::write(&short, b"short").unwrap();
        assert!(!manager.import_cache(&info, &short).await);
        assert_eq!(std::fs::read(info.to_path(root.path())).unwrap(), bytes);
        assert_eq!(manager.metrics.cache_count.get(), 1);
    }
    #[tokio::test]
    async fn existing_unaccounted_files_and_stale_aggregates_cannot_underflow() {
        let root = tempfile::tempdir().unwrap();
        let manager = manager(root.path());
        let bytes = b"not-yet-scanned";
        let hash = const_hex::encode(digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, bytes));
        let info = CacheFileInfo::from_file_id(format!("{hash}-{}-jpg", bytes.len())).unwrap();
        let path = info.to_path(root.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
        assert!(manager.remove_cache(&info).await.is_some());
        assert_eq!(manager.metrics.cache_count.get(), 0);
        assert_eq!(manager.metrics.cache_size.get(), 0);
        std::fs::write(&path, bytes).unwrap();
        let source = manager.create_temp_file().await.unwrap();
        std::fs::write(&source, bytes).unwrap();
        assert!(manager.import_cache(&info, &source).await);
        assert_eq!(manager.metrics.cache_count.get(), 1);
        manager.metrics.cache_count.set(0);
        manager.metrics.cache_size.set(0);
        assert!(manager.remove_cache(&info).await.is_some());
        assert_eq!(manager.metrics.cache_count.get(), 0);
        assert_eq!(manager.metrics.cache_size.get(), 0);
        let mut state = CacheState::default();
        assert_eq!(state.remove_file(u64::MAX), (0, 0));
        assert_eq!(state.file_count, 0);
        assert_eq!(state.size, 0);
    }
    fn manager(root: &Path) -> CacheManager {
        CacheManager {
            mutations: tokio::sync::Mutex::new(()),
            jobs: crate::cache_jobs::CacheJobs::new(),
            cache_dir: root.into(),
            cache_state: Default::default(),
            cache_state_path: root.join("state"),
            temp_dir: root.into(),
            size_limit: AtomicU64::new(u64::MAX),
            io_pool: None,
            metrics: Arc::new(Metrics::new()),
            background: Mutex::new(None),
            background_stop: CancellationToken::new(),
            scan_complete: AtomicBool::new(false),
        }
    }
    #[tokio::test]
    async fn interrupted_scan_cannot_truncate_saved_state() {
        let root = tempfile::tempdir().unwrap();
        let manager = manager(root.path());
        std::fs::write(&manager.cache_state_path, b"last-complete-state").unwrap();
        manager.save_state().await;
        assert_eq!(std::fs::read(&manager.cache_state_path).unwrap(), b"last-complete-state");
        manager.scan_complete.store(true, Relaxed);
        manager.save_state().await;
        assert!(read_state(&manager.cache_state_path).await.unwrap().is_empty());
    }
    #[tokio::test]
    async fn background_maintenance_is_joined_before_shutdown_returns() {
        let root = tempfile::tempdir().unwrap();
        let manager = Arc::new(manager(root.path()));
        CacheManager::start_background_task(manager.clone(), None);
        tokio::time::timeout(Duration::from_secs(2), manager.shutdown_background())
            .await
            .unwrap();
        assert!(manager.background.lock().is_none());
        assert!(manager.background_stop.is_cancelled());
    }

    #[tokio::test]
    async fn failed_scan_does_not_advertise_complete_cache_statistics() {
        let root = tempfile::tempdir().unwrap();
        let mut manager = manager(root.path());
        manager.cache_dir = root.path().join("cache");
        assert!(manager.scan_cache(Vec::new(), 1, false).await.is_err());
        assert!(!manager.metrics.cache_scan_complete.load(Relaxed));
        std::fs::create_dir(&manager.cache_dir).unwrap();
        manager.scan_cache(Vec::new(), 1, false).await.unwrap();
        assert!(manager.metrics.cache_scan_complete.load(Relaxed));
    }
}

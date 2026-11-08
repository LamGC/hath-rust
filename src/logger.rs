use chrono::{SecondsFormat, Utc};
use log::{Level, LevelFilter, Metadata, Record, SetLoggerError};
use std::{
    fmt::{self, Write as _},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};
use tokio::{
    fs::{File, OpenOptions, rename, symlink_metadata},
    io::{AsyncWriteExt, BufWriter, stderr, stdout},
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinHandle,
};

const RECORDS: usize = 128;
const LINE_BYTES: usize = 2048;
const FILE_BYTES: u64 = 4 * 1024 * 1024;
const SAFE_TARGET: &str = "hath_manager.safe";

pub struct Logger {
    config: Arc<LoggerConfig>,
    worker: Arc<LoggerWorker>,
    handle: Option<JoinHandle<()>>,
}
pub struct LoggerConfig {
    write_info: AtomicBool,
    flush: AtomicBool,
    console_level: AtomicU8,
    managed: bool,
    dropped: AtomicU64,
    redacted: AtomicU64,
    write_failures: AtomicU64,
    rate: [AtomicU64; 12],
    since: Instant,
}
impl LoggerConfig {
    fn new(managed: bool) -> Self {
        Self {
            write_info: AtomicBool::new(true),
            flush: AtomicBool::new(false),
            console_level: AtomicU8::new(0),
            managed,
            dropped: AtomicU64::new(0),
            redacted: AtomicU64::new(0),
            write_failures: AtomicU64::new(0),
            rate: std::array::from_fn(|_| AtomicU64::new(0)),
            since: Instant::now(),
        }
    }
    pub fn write_info(&self, value: bool) -> &Self {
        self.write_info.store(value, Relaxed);
        self
    }
    pub fn flush(&self, value: bool) -> &Self {
        self.flush.store(value, Relaxed);
        self
    }
    pub fn console_level(&self, value: u8) -> &Self {
        self.console_level.store(value, Relaxed);
        self
    }
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    pub(crate) fn counters(&self) -> [u64; 3] {
        [
            self.dropped.load(Relaxed),
            self.redacted.load(Relaxed),
            self.write_failures.load(Relaxed),
        ]
    }
}
impl Default for LoggerConfig {
    fn default() -> Self {
        Self::new(false)
    }
}
pub(crate) enum Diagnostic {
    Starting,
    LoginFailed,
    LoggedIn,
    CacheReady,
    CacheImportFailed,
    HttpReady,
    UpstreamReady,
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    RuntimeUnavailable,
    Stopping,
    Stopped,
    StartupFailed,
}
impl Diagnostic {
    fn code(self) -> &'static str {
        match self {
            Self::Starting => "client.starting",
            Self::LoginFailed => "client.login.failed",
            Self::LoggedIn => "client.logged_in",
            Self::CacheReady => "cache.initialized",
            Self::CacheImportFailed => "cache.import_failed",
            Self::HttpReady => "http.bound",
            Self::UpstreamReady => "client.upstream_ready",
            #[cfg(all(feature = "manager-integration", target_os = "linux"))]
            Self::RuntimeUnavailable => "integration.unavailable",
            Self::Stopping => "client.stopping",
            Self::Stopped => "client.stopped",
            Self::StartupFailed => "client.startup_failed",
        }
    }
}
pub(crate) fn event(code: Diagnostic) {
    log::info!(target:SAFE_TARGET,"event={}",code.code());
}
struct LoggerMessage {
    level: Level,
    message: String,
    _permit: OwnedSemaphorePermit,
}
struct LoggerWorker {
    config: Arc<LoggerConfig>,
    flush: Arc<Notify>,
    shutdown: Arc<Notify>,
    budget: Arc<Semaphore>,
    tx: mpsc::Sender<LoggerMessage>,
}

impl Logger {
    pub fn init<P: AsRef<Path>>(path: P, managed: bool) -> Result<Self, SetLoggerError> {
        let config = Arc::new(LoggerConfig::new(managed));
        let (worker, handle) = LoggerWorker::new(path, config.clone());
        let worker = Arc::new(worker);
        let logger = Self {
            config,
            worker: worker.clone(),
            handle: Some(handle),
        };
        log::set_boxed_logger(Box::new(worker))
            .map(|()| log::set_max_level(LevelFilter::Debug))
            .map(|()| logger)
    }
    pub fn config(&self) -> Arc<LoggerConfig> {
        self.config.clone()
    }
    pub async fn shutdown(&mut self) {
        self.worker.shutdown.notify_one();
        if let Some(mut handle) = self.handle.take()
            && tokio::time::timeout(Duration::from_secs(2), &mut handle).await.is_err()
        {
            handle.abort();
            let _ = handle.await;
        }
    }
}
impl Drop for Logger {
    fn drop(&mut self) {
        self.worker.shutdown.notify_one();
    }
}

struct BoundedLine {
    text: String,
    truncated: bool,
}
impl BoundedLine {
    fn new() -> Self {
        Self {
            text: String::with_capacity(LINE_BYTES),
            truncated: false,
        }
    }
    fn finish(mut self) -> String {
        if self.truncated {
            const MARKER: &str = "...[truncated]";
            let mut end = self.text.len().min(LINE_BYTES - MARKER.len());
            while !self.text.is_char_boundary(end) {
                end -= 1;
            }
            self.text.truncate(end);
            self.text.push_str(MARKER);
        }
        self.text
    }
}
impl fmt::Write for BoundedLine {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        for character in value.chars() {
            let character = if character.is_control() { ' ' } else { character };
            if self.text.len() + character.len_utf8() > LINE_BYTES {
                self.truncated = true;
                return Err(fmt::Error);
            }
            self.text.push(character);
        }
        Ok(())
    }
}
fn add(counter: &AtomicU64) {
    let _ = counter.try_update(Relaxed, Relaxed, |value| Some(value.saturating_add(1)));
}
fn source(target: &str) -> (usize, &'static str) {
    if target.contains("middleware") || target.contains("server") || (target.contains("route") && !target.contains("gallery")) {
        (0, "http")
    } else if target.contains("rpc") {
        (1, "rpc")
    } else if target.contains("cache") {
        (2, "cache")
    } else if target.contains("gallery") {
        (3, "gallery")
    } else if target.contains("manager_integration") {
        (4, "integration")
    } else {
        (5, "other")
    }
}
impl log::Log for LoggerWorker {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Debug
    }
    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let redacted = self.config.managed && record.target() != SAFE_TARGET;
        let (index, category) = source(record.target());
        if redacted {
            add(&self.config.redacted);
            if record.level() > Level::Warn {
                return;
            }
            let index = index * 2 + usize::from(record.level() == Level::Warn);
            let now = self.config.since.elapsed().as_secs().saturating_add(1);
            if self.config.rate[index]
                .try_update(Relaxed, Relaxed, |previous| {
                    if previous == 0 || now.saturating_sub(previous) >= 30 {
                        Some(now)
                    } else {
                        None
                    }
                })
                .is_err()
            {
                return;
            }
        }
        let Ok(permit) = self.budget.clone().try_acquire_owned() else {
            add(&self.config.dropped);
            return;
        };
        let mut line = BoundedLine::new();
        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        if redacted {
            let _ = write!(&mut line, "{timestamp} [{}] event=diagnostic.redacted source={category}", record.level().as_str());
        } else if self.config.managed {
            let _ = write!(&mut line, "{timestamp} [{}] {}", record.level().as_str(), record.args());
        } else {
            let _ = write!(&mut line, "{timestamp} [{}/{}] {}", record.level().as_str(), record.target(), record.args());
        }
        if self
            .tx
            .try_send(LoggerMessage {
                level: record.level(),
                message: line.finish(),
                _permit: permit,
            })
            .is_err()
        {
            add(&self.config.dropped);
        }
    }
    fn flush(&self) {
        self.flush.notify_one();
    }
}
struct LogFile {
    path: PathBuf,
    file: BufWriter<File>,
    bytes: u64,
    managed: bool,
}
impl LogFile {
    async fn open(path: PathBuf, managed: bool) -> std::io::Result<Self> {
        if let Ok(metadata) = symlink_metadata(&path).await {
            validate_log(&metadata, managed)?;
            let old = path.with_extension("old");
            if let Ok(metadata) = symlink_metadata(&old).await {
                validate_log(&metadata, managed)?;
            }
            rename(&path, &old).await?;
        }
        let file = open_log(&path, managed).await?;
        Ok(Self {
            path,
            file: BufWriter::new(file),
            bytes: 0,
            managed,
        })
    }
    async fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        if bytes.len() as u64 > FILE_BYTES {
            return Err(std::io::Error::other("log_record_too_large"));
        }
        if self.bytes + bytes.len() as u64 > FILE_BYTES {
            self.file.flush().await?;
            let old = self.path.with_extension("old");
            // Do not follow or truncate existing aliases, including the rotation target.
            if let Ok(metadata) = symlink_metadata(&old).await {
                validate_log(&metadata, self.managed)?;
            }
            rename(&self.path, &old).await?;
            self.file = BufWriter::new(open_log(&self.path, self.managed).await?);
            self.bytes = 0;
        }
        // Reserve before IO: a partial write followed by an error must not evade the cap.
        self.bytes += bytes.len() as u64;
        self.file.write_all(bytes).await?;
        Ok(())
    }
}
async fn open_log(path: &Path, managed: bool) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).write(true);
    #[cfg(unix)]
    {
        options
            .mode(0o600)
            .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32);
    }
    let file = options.open(path).await?;
    let metadata = file.metadata().await?;
    validate_log(&metadata, managed)?;
    file.set_len(0).await?;
    Ok(file)
}
fn validate_log(metadata: &std::fs::Metadata, managed: bool) -> std::io::Result<()> {
    if !metadata.is_file() {
        return Err(std::io::Error::other("log_file_invalid"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 || managed && (metadata.uid() != rustix::process::geteuid().as_raw() || metadata.mode() & 0o7777 != 0o600)
        {
            return Err(std::io::Error::other("log_file_invalid"));
        }
    }
    #[cfg(not(unix))]
    let _ = managed;
    Ok(())
}
impl LoggerWorker {
    fn new<P: AsRef<Path>>(path: P, config: Arc<LoggerConfig>) -> (Self, JoinHandle<()>) {
        let flush = Arc::new(Notify::new());
        let shutdown = Arc::new(Notify::new());
        let budget = Arc::new(Semaphore::new(RECORDS));
        let (tx, mut rx) = mpsc::channel::<LoggerMessage>(RECORDS);
        let root = path.as_ref().to_owned();
        let flush_signal = flush.clone();
        let stop_signal = shutdown.clone();
        let stats = config.clone();
        let task = tokio::spawn(async move {
            let mut error = match LogFile::open(root.join("log_err"), config.managed).await {
                Ok(file) => file,
                Err(_) => {
                    add(&config.write_failures);
                    rx.close();
                    return;
                }
            };
            let mut output = match LogFile::open(root.join("log_out"), config.managed).await {
                Ok(file) => file,
                Err(_) => {
                    add(&config.write_failures);
                    rx.close();
                    return;
                }
            };
            let mut console_out = stdout();
            let mut console_err = stderr();
            loop {
                tokio::select! {
                    biased;
                    _=stop_signal.notified()=>rx.close(),
                    item=rx.recv()=>{
                        let Some(item)=item else {break};
                        let console_level=match config.console_level.load(Relaxed){0=>LevelFilter::Debug,1=>LevelFilter::Info,2=>LevelFilter::Warn,3=>LevelFilter::Error,_=>LevelFilter::Off};
                        let mut bytes=item.message.into_bytes();bytes.push(b'\n');
                        if item.level<=Level::Warn {
                            if item.level<=console_level&&console_err.write_all(&bytes).await.is_err(){add(&config.write_failures);}
                            if error.append(&bytes).await.is_err(){add(&config.write_failures);}
                        } else {
                            if item.level<=console_level&&console_out.write_all(&bytes).await.is_err(){add(&config.write_failures);}
                            if config.write_info.load(Relaxed){
                                if output.append(&bytes).await.is_err(){add(&config.write_failures);}
                                if config.flush.load(Relaxed)&&output.file.flush().await.is_err(){add(&config.write_failures);}
                            }
                        }
                        // The record permit lives through the in-flight disk/console write.
                        drop(item._permit);
                    },
                    _=flush_signal.notified()=>{if output.file.flush().await.is_err(){add(&config.write_failures);}}
                }
            }
            if output.file.flush().await.is_err() {
                add(&config.write_failures);
            }
            if error.file.flush().await.is_err() {
                add(&config.write_failures);
            }
        });
        (
            Self {
                config: stats,
                flush,
                shutdown,
                budget,
                tx,
            },
            task,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::Log;
    #[tokio::test(flavor = "current_thread")]
    async fn drop_does_not_block_and_shutdown_drains_the_queue() {
        let root = tempfile::tempdir().unwrap();
        let config = Arc::new(LoggerConfig::default());
        config.console_level(4);
        let (worker, handle) = LoggerWorker::new(root.path(), config.clone());
        worker.log(&Record::builder().level(Level::Warn).args(format_args!("last diagnostic")).build());
        let logger = Logger {
            config,
            worker: Arc::new(worker),
            handle: None,
        };
        drop(logger);
        tokio::time::timeout(Duration::from_secs(2), handle).await.unwrap().unwrap();
        assert!(
            std::fs::read_to_string(root.path().join("log_err"))
                .unwrap()
                .contains("last diagnostic")
        );
    }
    #[test]
    fn queue_and_unicode_lines_are_bounded_before_formatting() {
        let config = Arc::new(LoggerConfig::default());
        let (tx, mut rx) = mpsc::channel(RECORDS);
        let worker = LoggerWorker {
            config: config.clone(),
            flush: Arc::new(Notify::new()),
            shutdown: Arc::new(Notify::new()),
            budget: Arc::new(Semaphore::new(RECORDS)),
            tx,
        };
        for _ in 0..10000 {
            worker.log(
                &Record::builder()
                    .level(Level::Info)
                    .args(format_args!("{}", "画".repeat(10000)))
                    .build(),
            );
        }
        assert_eq!(rx.len(), RECORDS);
        assert_eq!(config.dropped.load(Relaxed), 10000 - RECORDS as u64);
        while let Ok(item) = rx.try_recv() {
            assert!(item.message.len() <= LINE_BYTES);
            assert!(!item.message.contains('\n'));
            assert!(item.message.ends_with("...[truncated]"));
        }
    }
    #[test]
    fn managed_diagnostics_never_format_raw_messages() {
        struct Secret;
        impl fmt::Display for Secret {
            fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
                panic!("untrusted formatting must not run");
            }
        }
        let config = Arc::new(LoggerConfig::new(true));
        let (tx, mut rx) = mpsc::channel(RECORDS);
        let worker = LoggerWorker {
            config: config.clone(),
            flush: Arc::new(Notify::new()),
            shutdown: Arc::new(Notify::new()),
            budget: Arc::new(Semaphore::new(RECORDS)),
            tx,
        };
        for _ in 0..10000 {
            worker.log(
                &Record::builder()
                    .level(Level::Error)
                    .target("hath_rust::rpc")
                    .args(format_args!("{}", Secret))
                    .build(),
            );
        }
        assert_eq!(rx.len(), 1);
        assert_eq!(config.redacted.load(Relaxed), 10000);
        assert_eq!(rx.try_recv().unwrap().message.split("event=").nth(1).unwrap(), "diagnostic.redacted source=rpc");
    }
    #[tokio::test]
    async fn rotation_is_byte_bounded_and_aliases_are_rejected() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("log");
        let mut file = LogFile::open(path.clone(), true).await.unwrap();
        for _ in 0..(FILE_BYTES / 1024 + 4) {
            file.append(&[b'x'; 1024]).await.unwrap();
        }
        file.file.flush().await.unwrap();
        assert!(std::fs::metadata(&path).unwrap().len() <= FILE_BYTES);
        assert!(std::fs::metadata(path.with_extension("old")).unwrap().len() <= FILE_BYTES);
        #[cfg(unix)]
        {
            let target = root.path().join("target");
            std::fs::write(&target, b"keep").unwrap();
            let alias = root.path().join("alias");
            std::os::unix::fs::symlink(&target, &alias).unwrap();
            assert!(open_log(&alias, true).await.is_err());
            assert_eq!(std::fs::read(target).unwrap(), b"keep");
        }
    }
}

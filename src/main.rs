#![windows_subsystem = "windows"]
#[cfg(not(target_env = "msvc"))]
use std::ffi::{CStr, c_char};
use std::{
    collections::HashMap,
    error::Error,
    net::{IpAddr, Ipv4Addr},
    ops::RangeInclusive,
    path::Path,
    sync::Arc,
    time::Duration,
};

use clap::Parser;
use const_format::formatcp;
use futures::TryFutureExt;
use inquire::{
    CustomType, Text,
    validator::{ErrorMessage, Validation},
};
use log::{error, info, warn};
use parking_lot::Mutex;
use regex_lite::Regex;
use reqwest::Proxy;
use tempfile::TempPath;
use tokio::{
    fs::{self, File, try_exists},
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, stderr, stdin},
    sync::{
        mpsc::{self, Sender},
        watch,
    },
    time::{Instant, MissedTickBehavior, interval, sleep},
};

use crate::{
    cache_manager::{CacheConfig, CacheFileInfo, CacheManager},
    error::Error::ApiResponseFail,
    gallery_downloader::GalleryDownloader,
    logger::{Diagnostic, Logger},
    metrics::Metrics,
    rpc::RPCClient,
    server::{Server, ServerOptions},
    util::{create_dirs, create_http_client},
};

mod cache_jobs;
mod cache_manager;
mod client_lifecycle;
mod error;
mod file_reader;
mod gallery_downloader;
mod logger;
#[cfg(feature = "manager-integration")]
mod manager_integration;
mod metrics;
mod middleware;
mod route;
mod rpc;
mod server;
mod util;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// jemalloc config
#[cfg(not(target_env = "msvc"))]
const JEMALLOC_CONF: &CStr = c"percpu_arena:phycpu,tcache:false,thp:never,dirty_decay_ms:1000,muzzy_decay_ms:0";
#[allow(non_upper_case_globals)]
#[cfg(not(any(target_env = "msvc", target_os = "android", target_os = "macos")))]
#[unsafe(no_mangle)]
pub static mut malloc_conf: *const c_char = JEMALLOC_CONF.as_ptr();
#[allow(non_upper_case_globals)]
#[cfg(any(target_os = "android", target_os = "macos"))]
#[unsafe(no_mangle)]
pub static mut _rjem_malloc_conf: *const c_char = JEMALLOC_CONF.as_ptr();

const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "-", env!("VERGEN_GIT_SHA"));
const COMPAT_VERSION: &str = "1.6.6";
pub const CLIENT_VERSION: &str = formatcp!("{COMPAT_VERSION} (Rust {})", env!("CARGO_PKG_VERSION")); // Public version string
const MAX_KEY_TIME_DRIFT: RangeInclusive<i64> = -300..=300;

#[derive(Parser)]
#[command(version = VERSION)]
struct Args {
    /// Describe the compiled hath-manager integration without starting the client
    #[cfg(feature = "manager-integration")]
    #[arg(long)]
    manager_describe: bool,

    /// Emit the integration descriptor as JSON
    #[cfg(feature = "manager-integration")]
    #[arg(long, requires = "manager_describe")]
    json: bool,

    /// Private hath-manager runtime configuration (not a ClientKey)
    #[cfg(feature = "manager-integration")]
    #[arg(long)]
    manager_config: Option<std::path::PathBuf>,
    /// Offline recovery for a fenced historical handoff; takes the spool writer lock
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    #[arg(long,requires_all=["manager_instance_id","manager_recovery_root"],conflicts_with_all=["manager_config","manager_describe","manager_recover_spool"])]
    manager_recover_gallery: Option<std::path::PathBuf>,
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    #[arg(long,requires="manager_instance_id",conflicts_with_all=["manager_config","manager_describe","manager_recover_gallery"])]
    manager_recover_spool: Option<std::path::PathBuf>,
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    #[arg(long)]
    manager_instance_id: Option<String>,
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    #[arg(long, requires = "manager_recover_gallery")]
    manager_recovery_root: Option<std::path::PathBuf>,
    /// Overrides the port set in the client's settings
    #[arg(short, long)]
    port: Option<u16>,

    /// Bound host for socket, and local address for outgoing requests (e.g. RPC calls, cache fills)
    #[arg(long)]
    host: Option<std::net::IpAddr>,

    /// Cache data location
    #[arg(long, default_value_t = String::from("cache"))]
    cache_dir: String,

    /// Login data location
    #[arg(long, default_value_t = String::from("data"))]
    data_dir: String,

    /// Downloader save location
    #[arg(long, default_value_t = String::from("download"))]
    download_dir: String,

    /// Logs location
    #[arg(long, default_value_t = String::from("log"))]
    log_dir: String,

    /// Temporary location for proxy request
    #[arg(long, default_value_t = String::from("tmp"))]
    temp_dir: String,

    /// Disable writing non-error logs to file
    #[arg(long, default_value_t = false)]
    disable_logging: bool,

    /// Flush the logs to disk every line
    #[arg(long, default_value_t = false)]
    flush_log: bool,

    /// Override the max connection soft limit, should only be used in special cases
    #[arg(long, default_value_t = 0)]
    max_connection: u64,

    /// Disable server command ip check, also disable flood control
    #[arg(long, default_value_t = false)]
    disable_ip_origin_check: bool,

    /// Disable flood control
    #[arg(long, default_value_t = false)]
    disable_flood_control: bool,

    /// Configure proxy for fetch cache
    #[arg(long)]
    proxy: Option<String>,

    /// Force background cache scan, even if verify cache integrity is enabled
    #[arg(long, default_value_t = false)]
    force_background_scan: bool,

    /// Quiet console output (specify multiple times to be quieter)
    #[arg(short, action = clap::ArgAction::Count)]
    quiet: u8,

    /// Override bootstrap RPC server IP, used if rpc.hentaiathome.net cannot be resolved or is blocked.
    #[arg(long)]
    rpc_server_ip: Option<String>,

    /// Enable metrics endpoint
    #[arg(long, default_value_t = false)]
    enable_metrics: bool,

    /// Deprecated: replace by --disable-server-header
    #[arg(long, default_value_t = false, hide = true)]
    sni_strict: bool,

    /// Disable sending "Server" header to avoid being discovered by scanners.
    #[arg(long, default_value_t = false)]
    disable_server_header: bool,

    /// Experimental HTTP3
    #[arg(long, default_value_t = false)]
    enable_h3: bool,
}

type DownloadState = Mutex<HashMap<[u8; 20], (watch::Receiver<Option<Arc<TempPath>>>, Arc<watch::Sender<u64>>)>>;

pub struct AppState {
    reqwest: reqwest::Client,
    rpc: Arc<RPCClient>,
    download_state: DownloadState,
    cache_manager: Arc<CacheManager>,
    command_channel: Sender<Command>,
    has_proxy: bool,
    metrics: Arc<Metrics>,
    local_addr: Option<IpAddr>,
}

pub enum Command {
    ReloadCert,
    RefreshSettings,
    StartDownloader,
    Overload,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Windows system tray
    if cfg!(windows) {
        build_tray_icon();
    }

    // Options
    let args = Args::try_parse();
    if let Err(ref err) = args {
        let _ = err.print();
        if cfg!(windows) {
            let mut out = stderr();
            let _ = out.write(b"\r\nPress Enter to exit...").await.unwrap();
            out.flush().await.unwrap();
            let _ = stdin().read(&mut [0]).await.unwrap();
        }
        std::process::exit(err.exit_code());
    }
    let args = args.unwrap();

    #[cfg(feature = "manager-integration")]
    if args.manager_describe {
        manager_integration::print_descriptor(args.json)?;
        return Ok(());
    }
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    if let Some(directory) = &args.manager_recover_gallery {
        manager_integration::print_recovered_handoff(
            directory,
            args.manager_instance_id.as_deref().unwrap(),
            args.manager_recovery_root.as_deref().unwrap(),
        )?;
        return Ok(());
    }
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    if let Some(root) = &args.manager_recover_spool {
        manager_integration::recover_spool_for_parent(root, args.manager_instance_id.as_deref().unwrap())?;
        return Ok(());
    }
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    if args.manager_instance_id.is_some() {
        return Err("Recovery instance requires an offline recovery operation".into());
    }
    #[cfg(all(feature = "manager-integration", not(target_os = "linux")))]
    if args.manager_config.is_some() {
        return Err("Manager runtime requires Linux".into());
    }

    #[cfg(feature = "manager-integration")]
    let managed_logging = args.manager_config.is_some();
    #[cfg(not(feature = "manager-integration"))]
    let managed_logging = false;
    if managed_logging {
        std::panic::set_hook(Box::new(|panic| {
            let line = panic.location().map(|location| location.line()).unwrap_or(0);
            eprintln!("event=client.panic line={line} details=redacted");
        }));
    }
    let mut lifetime = client_lifecycle::ClientLifetime::default();
    let signals = client_lifecycle::ShutdownSignals::install()?;
    let (shutdown_send, shutdown_recv) = mpsc::unbounded_channel::<()>();
    let signal_stop = lifetime.stop.clone();
    lifetime.signals = Some(tokio::spawn(async move {
        signals.wait(shutdown_recv).await;
        signal_stop.cancel();
    }));
    let stop = lifetime.stop.clone();
    let result =
        tokio::select! { biased; _ = stop.cancelled() => Ok(()), result = run_client(args, shutdown_send, &mut lifetime) => result };
    if result.is_err() {
        logger::event(Diagnostic::StartupFailed);
    }
    lifetime.shutdown().await;
    if managed_logging {
        result.map_err(|error| std::io::Error::other(safe_failure(error.as_ref())).into())
    } else {
        result
    }
}

async fn run_client(
    args: Args,
    shutdown_send: mpsc::UnboundedSender<()>,
    lifetime: &mut client_lifecycle::ClientLifetime,
) -> Result<(), Box<dyn Error>> {
    // RPCClient constructs the same URL synchronously. Reject invalid input here
    // before acquiring integration resources instead of unwinding through them.
    if let Some(server) = &args.rpc_server_ip {
        reqwest::Url::parse(&format!("http://{server}/16/rpc")).map_err(|_| "Invalid RPC bootstrap address")?;
    }
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    let managed_credentials = if let Some(path) = &args.manager_config {
        let (managed, credentials) = manager_integration::ManagedRuntime::start(path, Path::new(&args.data_dir)).await?;
        lifetime.managed = Some(managed);
        Some(credentials)
    } else {
        None
    };

    create_dirs(vec![&args.data_dir, &args.cache_dir, &args.log_dir, &args.temp_dir, &args.download_dir]).await?;

    // Init logger
    #[cfg(feature = "manager-integration")]
    let managed_logging = args.manager_config.is_some();
    #[cfg(not(feature = "manager-integration"))]
    let managed_logging = false;
    lifetime.logger = Some(Logger::init(&args.log_dir, managed_logging)?);
    let logger = lifetime.logger.as_ref().unwrap();
    logger
        .config()
        .write_info(!args.disable_logging)
        .flush(args.flush_log)
        .console_level(args.quiet);

    let mut metrics = Metrics::new();
    metrics.logs = Arc::downgrade(&logger.config());
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    let metrics = {
        let mut metrics = metrics;
        metrics.observation = lifetime.managed.as_ref().map(|owner| owner.observations()).unwrap_or_default();
        metrics
    };
    let metrics = Arc::new(metrics);

    info!("Hentai@Home {COMPAT_VERSION} (Rust {VERSION}) starting up");
    logger::event(Diagnostic::Starting);
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    if lifetime.managed.as_ref().is_some_and(|owner| owner.is_degraded()) {
        logger::event(Diagnostic::RuntimeUnavailable);
        error!(
            "Manager durable runtime unavailable; Gallery is paused, cache service may continue. Repair storage and restart the client."
        );
    }

    // Deprecated option handle
    if args.sni_strict {
        error!("Option --sni-strict deprecated, use --disable_server_header instead.");
        // args.disable_server_header = true;
        // TODO remove sni-strict function
    }

    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    let credential = match managed_credentials {
        Some(credentials) => Some(credentials),
        None => read_credential(&args.data_dir).await?,
    };
    #[cfg(not(all(feature = "manager-integration", target_os = "linux")))]
    let credential = read_credential(&args.data_dir).await?;
    let (id, key) = match credential {
        Some(i) => i,
        None => setup(&args.data_dir).await?,
    };
    let client = RPCClient::new(id, &key, args.disable_ip_origin_check, args.max_connection, args.rpc_server_ip.as_deref(), args.host);
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    let client = client.with_observation(metrics.observation.clone());
    let client = Arc::new(client);
    lifetime.client = Some(client.clone());
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    if let Some(owner) = &mut lifetime.managed {
        owner.observe(Arc::downgrade(&metrics), Arc::downgrade(&client));
    }
    let init_settings = match client.login().await {
        Ok(settings) => settings,
        Err(err) => {
            logger::event(Diagnostic::LoginFailed);
            error!("Login error: {}", err);
            if let ApiResponseFail { fail_code, .. } = &err
                && fail_code.starts_with("FAIL_STARTUP_LOOP")
            {
                warn!("Restart loop detected, wait 15 minutes.");
                sleep(Duration::from_mins(15)).await
            } else {
                sleep(Duration::from_secs(5)).await;
            }
            return Err(err.into());
        }
    };
    logger::event(Diagnostic::LoggedIn);

    let settings = client.settings();
    logger.config().write_info(!settings.disable_logging());
    let cache_manager = CacheManager::new(
        CacheConfig::new(args.data_dir, args.cache_dir, args.temp_dir),
        settings.clone(),
        &init_settings,
        args.force_background_scan,
        shutdown_send.clone(),
        metrics.clone(),
    )
    .await?;
    lifetime.cache = Some(cache_manager.clone());
    logger::event(Diagnostic::CacheReady);

    // Proxy
    let proxy = match args.proxy.as_ref().map(Proxy::all) {
        Some(Ok(proxy)) => {
            info!("Using proxy for fetch cache: {}", args.proxy.unwrap());
            Some(proxy)
        }
        Some(Err(err)) => {
            error!("Parser proxy setting error: {}", err);
            None
        }
        None => None,
    };
    // Command channel
    let (tx, mut rx) = mpsc::channel::<Command>(1);

    info!("Starting HTTP server...");
    let port = args.port.unwrap_or_else(|| init_settings.client_port());
    let cert = client.get_cert().await.ok_or("Failed to get server certificate")?;
    let state = AppState {
        reqwest: create_http_client(Duration::from_secs(30), proxy.clone(), args.host),
        rpc: client.clone(),
        download_state: Default::default(),
        cache_manager: cache_manager.clone(),
        command_channel: tx.clone(),
        has_proxy: proxy.is_some(),
        metrics: metrics.clone(),
        local_addr: args.host,
    };
    let flood_control = !(args.disable_flood_control || args.disable_ip_origin_check);
    let Some(server) = Server::new(
        ServerOptions::new(port, args.host.unwrap_or(Ipv4Addr::UNSPECIFIED.into()), cert, state)
            .flood_control(flood_control)
            .metrics(args.enable_metrics)
            .sni_strict(args.sni_strict)
            .server_header(!args.disable_server_header)
            .h3(args.enable_h3),
    ) else {
        error!("Server start failed.");
        sleep(Duration::from_secs(5)).await;
        return Err(error::Error::ServerStartFail.into());
    };
    let server_handle = server.handle();
    lifetime.server = Some(server);
    logger::event(Diagnostic::HttpReady);

    info!("Notifying the server that we have finished starting up the client...");
    if client.connect_check(init_settings).await.is_none() {
        error!("Startup notification failed.");
        sleep(Duration::from_secs(5)).await;
        return Err(error::Error::ConnectTestFail.into());
    }
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    if let Some(managed) = &lifetime.managed {
        managed.upstream_ready();
    }
    logger::event(Diagnostic::UpstreamReady);

    // Check download jobs
    client.refresh_settings().await;

    // Check purge list
    purge_cache(&client, &cache_manager, &metrics, 259200).await;

    info!("H@H initialization completed successfully. Starting normal operation");

    // Command listener
    let client2 = client.clone();
    let cache_manager2 = cache_manager.clone();
    let downloader2 = lifetime.downloader.clone();
    let gallery_stop2 = lifetime.stop.clone();
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    let gallery_integration = lifetime.managed.as_ref().map(|owner| owner.handle()).unwrap_or_default();
    let logger_config = logger.config();
    let metrics2 = metrics.clone();
    lifetime.dispatcher = Some(tokio::spawn(async move {
        let mut last_overload = Instant::now().checked_sub(Duration::from_secs(30)).unwrap_or_else(Instant::now);
        let mut retry_certificate = None;
        loop {
            let command = tokio::select! {
                biased;
                _ = gallery_stop2.cancelled() => break,
                command = rx.recv() => match command { Some(command) => command, None => break },
                _ = tokio::time::sleep_until(retry_certificate.unwrap_or_else(|| Instant::now() + Duration::from_secs(86400))), if retry_certificate.is_some() => Command::ReloadCert,
            };
            #[cfg(all(feature = "manager-integration", target_os = "linux"))]
            let mut observation = metrics2.observation.operation(match &command {
                Command::ReloadCert => manager_integration::Operation::ReloadCert,
                Command::RefreshSettings => manager_integration::Operation::RefreshSettings,
                Command::StartDownloader => manager_integration::Operation::StartDownloader,
                Command::Overload => manager_integration::Operation::Overload,
            });
            match command {
                Command::ReloadCert => {
                    retry_certificate = None;
                    match client2.get_cert().await {
                        Some(cert) => {
                            if let Err(err) = server_handle.update_cert(cert) {
                                #[cfg(all(feature = "manager-integration", target_os = "linux"))]
                                observation.result(false);
                                error!("Update SSL cert fail: {}", err);
                                // Retry after 10s
                                retry_certificate = Some(Instant::now() + Duration::from_secs(10));
                            }
                        }
                        None => {
                            #[cfg(all(feature = "manager-integration", target_os = "linux"))]
                            observation.result(false);
                            error!("Fetch SSL cert fail");
                            // Retry after 10s
                            retry_certificate = Some(Instant::now() + Duration::from_secs(10));
                        }
                    }
                }
                Command::RefreshSettings => {
                    client2.refresh_settings().await;
                    cache_manager2.update_settings(client2.settings());
                    if !args.disable_logging {
                        logger_config.write_info(!client2.settings().disable_logging());
                    }
                }
                Command::StartDownloader => {
                    if gallery_stop2.is_cancelled() || !client2.is_running() {
                        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
                        observation.finish(manager_integration::Outcome::Noop);
                        continue;
                    }
                    let previous = {
                        let mut slot = downloader2.lock();
                        if slot.as_ref().is_some_and(|job| job.is_finished()) {
                            slot.take()
                        } else {
                            None
                        }
                    };
                    if let Some(previous) = previous {
                        let _ = previous.await;
                    }
                    if downloader2.lock().is_none() {
                        let new = GalleryDownloader::new(client2.clone(), &args.download_dir, proxy.clone(), metrics2.clone(), args.host);
                        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
                        let new = new.with_integration(gallery_integration.clone());
                        let stop = gallery_stop2.clone();
                        *downloader2.lock() = Some(tokio::spawn(async move {
                            new.run(stop).await;
                        }))
                    } else {
                        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
                        observation.finish(manager_integration::Outcome::Noop);
                    }
                }
                Command::Overload => {
                    if last_overload.elapsed() > Duration::from_secs(30) {
                        last_overload = Instant::now();
                        warn!("Server overloaded!");
                        client2.notify_overload().await;
                    } else {
                        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
                        observation.finish(manager_integration::Outcome::Noop);
                    }
                }
            }
            #[cfg(all(feature = "manager-integration", target_os = "linux"))]
            observation.result(true);
        }
    }));

    // Schedule task
    let client3 = client.clone();
    let cache_manager3 = cache_manager.clone();
    let metrics3 = metrics.clone();
    lifetime.keepalive = Some(tokio::spawn(async move {
        let mut counter: u32 = 0;
        let mut interval = interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;

            if !client3.is_running() {
                break;
            }

            // Alive check every 110s
            if counter.is_multiple_of(11) && !client3.still_alive(false).await {
                let _ = shutdown_send.send(()); // Check fail, shutdown.
            }

            // Check purge list every 7hr
            if counter % 2160 == 2159 {
                purge_cache(&client3, &cache_manager3, &metrics3, 43200).await;
            }

            counter = counter.wrapping_add(1);
            log::logger().flush(); // Flush log every 10s
        }
    }));

    // Shutdown handle
    lifetime.wait().await?;
    info!("Shutting down...");
    Ok(())
}

/**
 * main helper
 */
async fn purge_cache(client: &RPCClient, cache: &CacheManager, metrics: &Metrics, delta: u64) {
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    let mut observation = metrics.observation.operation(manager_integration::Operation::CacheBlacklist);
    #[cfg(not(all(feature = "manager-integration", target_os = "linux")))]
    let _ = metrics;
    let list = client.get_purgelist(delta).await;
    let available = list.is_some();
    if let Some(list) = list {
        for info in list.iter().filter_map(CacheFileInfo::from_file_id) {
            cache.remove_cache(&info).await;
        }
    }
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    observation.result(available);
    let _ = available;
}
fn safe_failure(error: &(dyn Error + 'static)) -> &'static str {
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        #[cfg(feature = "manager-integration")]
        if let Some(code) = error
            .get_ref()
            .and_then(|error| error.downcast_ref::<manager_integration::FailureCode>())
        {
            return code.code();
        }
        return match error.kind() {
            std::io::ErrorKind::PermissionDenied => "native.io.permission_denied",
            std::io::ErrorKind::NotFound => "native.io.not_found",
            std::io::ErrorKind::AlreadyExists => "native.io.already_exists",
            std::io::ErrorKind::WouldBlock => "native.io.busy",
            _ => "native.io.failed",
        };
    }
    if let Some(error) = error.downcast_ref::<crate::error::Error>() {
        return match error {
            crate::error::Error::VersionTooOld => "client.version_too_old",
            crate::error::Error::ServerStartFail => "http.start_failed",
            crate::error::Error::ConnectTestFail => "client.startup_rejected",
            crate::error::Error::ApiResponseFail { fail_code, .. } => {
                if fail_code.starts_with("FAIL_STARTUP_LOOP") {
                    "client.restart_loop"
                } else if fail_code.starts_with("FAIL_CID_IN_USE") {
                    "client.identity_in_use"
                } else if fail_code.starts_with("FAIL_OTHER_CLIENT_CONNECTED") {
                    "client.other_client_connected"
                } else {
                    "client.rpc_failed"
                }
            }
            _ => "native.client_failed",
        };
    }
    "native.client_failed"
}
async fn read_credential<P: AsRef<Path>>(data_path: P) -> Result<Option<(i32, String)>, Box<dyn Error>> {
    let path = data_path.as_ref().join("client_login");
    if !try_exists(&path).await? {
        return Ok(None);
    }
    let data = match File::open(&path)
        .and_then(|f| async { BufReader::new(f).lines().next_line().await })
        .await
    {
        Ok(Some(data)) => data,
        Ok(None) => return Ok(None),
        Err(err) => {
            error!("Encountered error when reading client_login: {}", err);
            return Err(Box::new(err));
        }
    };
    let mut credential = data.split('-');

    let (id, key) = match credential.next().and_then(|s| s.parse::<i32>().ok()).zip(credential.next()) {
        Some(v) => v,
        None => return Ok(None),
    };

    info!("Loaded login settings from {}", path.display());
    Ok(Some((id, key.to_owned())))
}

async fn setup<P: AsRef<Path>>(data_path: P) -> Result<(i32, String), Box<dyn Error>> {
    info!("Setup client");
    sleep(Duration::from_secs(1)).await; // Wait logger

    println!(
        "
Before you can use this client, you will have to register it at https://e-hentai.org/hentaiathome.php
IMPORTANT: YOU NEED A SEPARATE IDENT FOR EACH CLIENT YOU WANT TO RUN.
DO NOT ENTER AN IDENT THAT WAS ASSIGNED FOR A DIFFERENT CLIENT UNLESS IT HAS BEEN RETIRED.
After registering, enter your ID and Key below to start your client.
(You will only have to do this once.)
"
    );

    let id = CustomType::<i32>::new("Enter Client ID:")
        .with_error_message("Invalid Client ID. Please try again.")
        .prompt()?;
    let key = Text::new("Enter Client Key:")
        .with_validator(|key: &_| {
            if Regex::new("^[a-zA-Z0-9]{20}$").unwrap().is_match(key) {
                Ok(Validation::Valid)
            } else {
                let message = "Invalid Client Key, it must be exactly 20 alphanumerical characters. Please try again.".to_owned();
                Ok(Validation::Invalid(ErrorMessage::Custom(message)))
            }
        })
        .prompt()?;

    // Write client_login
    let path = data_path.as_ref().join("client_login");
    if let Err(err) = fs::write(&path, format!("{}-{}", id, key)).await {
        error!("Error encountered when writing client_login: {}", err);
        return Err(Box::new(err));
    }

    Ok((id, key))
}

#[cfg(not(windows))]
fn build_tray_icon() {
    // Tray icon is windows only.
}

#[cfg(windows)]
fn build_tray_icon() {
    use std::thread;

    use tao::{
        event_loop::{ControlFlow, EventLoopBuilder},
        platform::windows::EventLoopBuilderExtWindows,
    };
    use tray_icon::{
        Icon, MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent,
        menu::{Menu, MenuEvent},
    };
    use windows::{
        Win32::{
            System::LibraryLoader::GetModuleHandleW,
            UI::WindowsAndMessaging::{GetSystemMetrics, IMAGE_ICON, LR_SHARED, LoadImageW, SM_CXSMICON},
        },
        core::PWSTR,
    };

    // Show console
    switch_window(false);

    thread::Builder::new()
        .name("TrayEvent".into())
        .spawn(|| {
            // Load icon
            let instence = unsafe { GetModuleHandleW(PWSTR::default()) }.map(Into::into).ok();
            let size = unsafe { GetSystemMetrics(SM_CXSMICON) };
            let icon = unsafe { LoadImageW(instence, PWSTR(1 as _), IMAGE_ICON, size, size, LR_SHARED) }.expect("Icon load failed");

            let tray_menu = Menu::new(); // TODO nemu
            let _tray_icon = TrayIconBuilder::new()
                .with_icon(Icon::from_handle(icon.0 as _))
                .with_menu(Box::new(tray_menu))
                .with_tooltip("hath-rust - Hentai@Home but rusty")
                .build()
                .unwrap();

            let mut console_hide = false;
            let event_loop = EventLoopBuilder::new().with_any_thread(true).build();
            let tray_channel = TrayIconEvent::receiver();
            let menu_channel = MenuEvent::receiver();
            event_loop.run(move |_event, _, control_flow| {
                *control_flow = ControlFlow::WaitUntil((Instant::now() + Duration::from_millis(100)).into());

                if let Ok(TrayIconEvent::Click {
                    id: _,
                    position: _,
                    rect: _,
                    button,
                    button_state,
                }) = tray_channel.try_recv()
                    && button == MouseButton::Left
                    && button_state == MouseButtonState::Up
                {
                    console_hide = !console_hide;
                    switch_window(console_hide)
                }
                if let Ok(_event) = menu_channel.try_recv() {
                    // TODO
                }
            });
        })
        .unwrap();
}

#[cfg(windows)]
fn switch_window(hide: bool) {
    use windows::{
        Win32::{
            Foundation::{LPARAM, WPARAM},
            System::{
                Console::{AllocConsole, GetConsoleWindow},
                LibraryLoader::GetModuleHandleW,
            },
            UI::WindowsAndMessaging::{
                DeleteMenu, GetSystemMenu, GetSystemMetrics, ICON_BIG, ICON_SMALL, IMAGE_ICON, LR_SHARED, LoadImageW, MF_BYCOMMAND,
                PostMessageW, SC_CLOSE, SM_CXICON, SM_CXSMICON, SW_HIDE, SW_SHOW, ShowWindow, WM_SETICON,
            },
        },
        core::PCWSTR,
    };

    let mut window = unsafe { GetConsoleWindow() };
    if window.is_invalid() && unsafe { AllocConsole().is_ok() } {
        window = unsafe { GetConsoleWindow() };

        // Try Disable close button
        let menu = unsafe { GetSystemMenu(window, false) };
        if !menu.is_invalid() {
            unsafe {
                let _ = DeleteMenu(menu, SC_CLOSE, MF_BYCOMMAND);
            };
        }
    }

    if !window.is_invalid() {
        // Switch window hide state
        let _ = unsafe { ShowWindow(window, if hide { SW_HIDE } else { SW_SHOW }) };

        // Workaround icon size
        if !hide {
            let instence = unsafe { GetModuleHandleW(PCWSTR::default()) }.map(Into::into).ok();
            let size_small = unsafe { GetSystemMetrics(SM_CXSMICON) };
            // Title icon
            if let Ok(icon) = unsafe { LoadImageW(instence, PCWSTR(1 as _), IMAGE_ICON, size_small, size_small, LR_SHARED) }
                && !icon.is_invalid()
            {
                let _ = unsafe { PostMessageW(Some(window), WM_SETICON, WPARAM(ICON_SMALL as _), LPARAM(icon.0 as _)) };
            }
            // Taskbar icon
            let size_large = unsafe { GetSystemMetrics(SM_CXICON) };
            if let Ok(icon) = unsafe { LoadImageW(instence, PCWSTR(1 as _), IMAGE_ICON, size_large, size_large, LR_SHARED) }
                && !icon.is_invalid()
            {
                let _ = unsafe { PostMessageW(Some(window), WM_SETICON, WPARAM(ICON_BIG as _), LPARAM(icon.0 as _)) };
            }
        }
    }
}

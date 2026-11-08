use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use aws_lc_rs::digest;
use const_hex::decode_to_array;
use futures::StreamExt;
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use regex_lite::Regex;
use reqwest::{Proxy, Url};
use tokio::{
    fs::{self, create_dir_all},
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinSet,
    time::{Instant, sleep_until},
};
use tokio_util::sync::CancellationToken;
use unicode_segmentation::UnicodeSegmentation;

#[cfg(all(feature = "manager-integration", target_os = "linux"))]
use crate::manager_integration::{FileReceipt, FileView, GalleryAttempt, GalleryView, IntegrationHandle};

use crate::{error::Error, metrics::Metrics, rpc::RPCClient, util};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const MAX_DOWNLOAD_TASK: u32 = 4;
#[cfg(all(feature = "manager-integration", target_os = "linux"))]
type GalleryObservation = Option<GalleryAttempt>;
#[cfg(not(all(feature = "manager-integration", target_os = "linux")))]
type GalleryObservation = ();

pub struct GalleryDownloader {
    client: Arc<RPCClient>,
    download_dir: PathBuf,
    proxy: Option<Proxy>,
    metrics: Arc<Metrics>,
    local_addr: Option<IpAddr>,
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    integration: IntegrationHandle,
}

impl GalleryDownloader {
    pub fn new<P: AsRef<Path>>(
        client: Arc<RPCClient>,
        download_dir: P,
        proxy: Option<Proxy>,
        metrics: Arc<Metrics>,
        local_addr: Option<IpAddr>,
    ) -> GalleryDownloader {
        GalleryDownloader {
            client,
            download_dir: download_dir.as_ref().to_path_buf(),
            proxy,
            metrics,
            local_addr,
            #[cfg(all(feature = "manager-integration", target_os = "linux"))]
            integration: IntegrationHandle::default(),
        }
    }

    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    pub fn with_integration(mut self, integration: IntegrationHandle) -> Self {
        self.integration = integration;
        self
    }

    pub async fn run(&self, stop: CancellationToken) {
        let stop = stop.child_token();
        let mut task = tokio::select! { biased; _ = stop.cancelled() => None, task = self.client.fetch_queue(None) => task }
            .map(|meta| self.parser(meta));

        'task: while let Some(meta) = task {
            if stop.is_cancelled() || !self.client.is_running() {
                break;
            }

            let meta = match meta {
                Ok(meta) => Arc::new(meta),
                Err(e) => {
                    warn!("Failed to parse metadata for new gallery. {}", e);
                    return;
                }
            };

            // Create directory
            let name_suffix = format!(" [{}{}]", meta.gid, meta.xres_title);
            let name_max_chars = 125 - name_suffix.len();
            let name_max_bytes = 255 - name_suffix.len();
            let name = if meta.title.chars().count() <= name_max_chars && meta.title.len() <= name_max_bytes {
                format!("{}{}", meta.title, name_suffix)
            } else {
                let mut truncate_pos = 0;
                // Search last grapheme pos within length limit
                for (index, (pos, _)) in meta.title.grapheme_indices(true).enumerate() {
                    if index + 3 > name_max_chars || pos + 3 > name_max_bytes {
                        break;
                    }
                    truncate_pos = pos;
                }
                format!("{}...{}", &meta.title[..truncate_pos], name_suffix)
            };
            let dir = match std::path::absolute(self.download_dir.join(name)) {
                Ok(dir) => dir,
                Err(_) => {
                    error!("Gallery download directory is invalid");
                    return;
                }
            };
            #[cfg(all(feature = "manager-integration", target_os = "linux"))]
            let mut attempt: Option<GalleryAttempt> = match self.integration.prepare_gallery(&dir, meta.as_ref()) {
                Ok(attempt) => attempt,
                Err(_) => {
                    error!("Gallery view validation failed");
                    return;
                }
            };
            #[cfg(not(all(feature = "manager-integration", target_os = "linux")))]
            let mut attempt = ();
            #[cfg(all(feature = "manager-integration", target_os = "linux"))]
            let dir = if let Some(attempt) = &mut attempt {
                let mut warning = None;
                loop {
                    if stop.is_cancelled() || !self.client.is_running() {
                        return;
                    }
                    if attempt.initialize().await.is_ok() {
                        break;
                    }
                    if !handoff_pause(&stop, &mut warning).await {
                        return;
                    }
                }
                attempt.directory().to_path_buf()
            } else {
                dir
            };
            if create_dir_all(&dir).await.is_err() {
                error!("Create gallery download directory failed");
                return;
            }

            self.metrics.download_count.inc();

            // Download files
            let downloaded_files = Arc::new(Mutex::new(HashMap::new()));
            'retry: for retry in 0..10 {
                let mut writers = JoinSet::new();
                let use_proxy = retry != 0 && self.proxy.is_some();
                let reqwest = if use_proxy {
                    util::create_http_client(Duration::from_secs(300), self.proxy.clone(), self.local_addr)
                } else {
                    util::create_http_client(Duration::from_secs(300), None, self.local_addr)
                };
                for info in &meta.gallery_files {
                    let info = info.clone();
                    if stop.is_cancelled() || !self.client.is_running() {
                        stop.cancel();
                        break;
                    }

                    // Check if file already downloaded
                    if downloaded_files.lock().contains_key(&info) {
                        continue;
                    }
                    let path = dir.join(format!("{}.{}", info.filename, info.filetype));
                    let verified = observe_while(
                        async {
                            tokio::select! { biased; _ = stop.cancelled() => false,
                            result = info.check_hash(&path) => result.unwrap_or(false) }
                        },
                        &mut attempt,
                    )
                    .await;
                    if verified && let Ok(metadata) = fs::metadata(&path).await {
                        downloaded_files.lock().insert(info, metadata.len());
                        continue;
                    }

                    // Get download URL and download file
                    let mut start_time = Instant::now();
                    let url = observe_while(
                        async {
                            tokio::select! { biased; _ = stop.cancelled() => None,
                            result = self.client.dl_fetch(meta.gid,info.page,info.fileindex,&info.xres,retry) => result }
                        },
                        &mut attempt,
                    )
                    .await
                    .and_then(|items| items.first().and_then(|value| Url::parse(value).ok()));
                    match url {
                        Some(url) => {
                            start_time = Instant::now();
                            while writers.len() >= MAX_DOWNLOAD_TASK as usize {
                                if observe_while(writers.join_next(), &mut attempt)
                                    .await
                                    .is_some_and(|result| result.is_err())
                                {
                                    warn!("Gallery writer task failed");
                                }
                            }
                            if stop.is_cancelled() || !self.client.is_running() {
                                stop.cancel();
                                break;
                            }
                            let meta = meta.clone();
                            let downloaded_files = downloaded_files.clone();
                            let mut reqwest = reqwest.clone();
                            let metrics = self.metrics.clone();
                            let local_addr = self.local_addr;
                            let stop = stop.clone();
                            writers.spawn(async move {
                                for retry in 0..3 {
                                    if stop.is_cancelled() {
                                        break;
                                    }
                                    let reqwest2 = reqwest.clone();
                                    let url2 = url.clone();
                                    let metrics2 = metrics.clone();
                                    let result = download(reqwest2, url2, &path, info.expected_sha1_hash, metrics2, &stop).await;
                                    if let Err(err) = &result {
                                        if stop.is_cancelled() {
                                            break;
                                        }
                                        warn!(
                                            "Gallery file download failed: gid={}, page={}, category={}",
                                            meta.gid,
                                            info.page,
                                            if err.is::<reqwest::Error>() {
                                                "network"
                                            } else if err.is::<Error>() {
                                                "integrity"
                                            } else {
                                                "io"
                                            }
                                        );

                                        // Try download without proxy at third time
                                        if retry == 1 && use_proxy {
                                            warn!("Retry download without proxy...");
                                            reqwest = util::create_http_client(Duration::from_secs(300), None, local_addr);
                                        }

                                        if retry == 2
                                            && (err.is::<reqwest::Error>() || err.is::<Error>())
                                            && let Some(host) = url.host_str()
                                        {
                                            meta.failures.lock().push(format!("{}-{}-{}", host, info.fileindex, info.xres))
                                        }
                                    } else {
                                        info!(
                                            "Finished downloading gid={} page={}: {}.{}",
                                            meta.gid, info.page, info.filename, info.filetype
                                        );
                                        downloaded_files.lock().insert(info, result.unwrap());
                                        break;
                                    }
                                }
                            });
                        }
                        None => {
                            warn!(
                                "Fetch gallery file download url fail. gid={}, page={}, fileindex={}",
                                meta.gid, info.page, info.fileindex
                            );
                        }
                    };

                    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
                    if let Some(attempt) = &mut attempt {
                        attempt.active().await;
                    }
                    tokio::select! { biased; _ = stop.cancelled() => (), _ = sleep_until(start_time + Duration::from_secs(1)) => () }
                }

                // Cooperative cancellation: never confuse aborting a parent with stopping writers.
                while let Some(result) = observe_while(writers.join_next(), &mut attempt).await {
                    if result.is_err() {
                        warn!("Gallery writer task failed");
                    }
                }
                if stop.is_cancelled() || !self.client.is_running() {
                    break 'task;
                }

                if downloaded_files.lock().len() == meta.filecount {
                    break 'retry;
                }
            }

            let success = downloaded_files.lock().len() == meta.filecount;
            #[cfg(all(feature = "manager-integration", target_os = "linux"))]
            let write_information = attempt.is_none();
            #[cfg(not(all(feature = "manager-integration", target_os = "linux")))]
            let write_information = true;
            #[cfg(all(feature = "manager-integration", target_os = "linux"))]
            if let Some(attempt) = &mut attempt {
                let mut warning = None;
                loop {
                    if stop.is_cancelled() || !self.client.is_running() {
                        return;
                    }
                    let receipts = downloaded_files
                        .lock()
                        .iter()
                        .map(|(file, size)| FileReceipt {
                            page: file.page,
                            size: *size,
                        })
                        .collect();
                    if attempt.finish(success, receipts).await.is_ok() {
                        break;
                    }
                    if !handoff_pause(&stop, &mut warning).await {
                        return;
                    }
                }
            }
            if success {
                if write_information && atomic_information(&dir, meta.information.clone()).await.is_err() {
                    error!("Could not commit galleryinfo.txt");
                    return;
                }
                info!("Finished download of gallery: {}", meta.title);
                let failures = meta.failures.lock().clone();
                tokio::select! { biased; _ = stop.cancelled() => return, _ = self.client.dl_fails(&failures) => () }
            } else {
                warn!("Permanently failed downloading gallery: {}", meta.title);
            }

            // This is the only queue-advance boundary; managed terminal metadata/event is durable first.
            task = tokio::select! { biased; _ = stop.cancelled() => None, result = self.client.fetch_queue(Some(&meta)) => result }
                .map(|meta| self.parser(meta));
        }
    }

    fn parser(&self, raw_gallery: Vec<String>) -> Result<GalleryMeta, BoxError> {
        GalleryMeta::parse(&raw_gallery, self.client.key())
    }
}

impl GalleryMeta {
    fn parse(raw_gallery: &[String], client_key: &str) -> Result<Self, BoxError> {
        debug!("GalleryDownloader: Started gallery metadata parsing");
        let allow_filetype = Regex::new(r#"^(jpg|gif|png|webp)$"#).unwrap();
        let allow_filename = Regex::new(r#"^[a-zA-Z0-9._-]{1,250}$"#).unwrap();

        let mut gid = 0;
        let mut filecount = 0;
        let mut minxres = String::new();
        let mut dl_hash = String::new();
        let mut title = String::new();
        let mut information = String::new();
        let mut gallery_files: Vec<Arc<GalleryFile>> = Vec::new();

        let mut parse_state = 0;
        let mut hasher = digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY);
        hasher.update(client_key.as_bytes());
        let mut headers = HashSet::new();
        let mut pages = HashSet::new();
        let mut paths = HashSet::new();
        if raw_gallery.len() > 10032 || raw_gallery.iter().map(String::len).sum::<usize>() > 8 * 1024 * 1024 {
            return Err("gallery_metadata_limit".into());
        }
        for s in raw_gallery {
            if s == "FILELIST" && parse_state < 2 {
                if parse_state != 0 {
                    return Err("gallery_metadata_sections".into());
                }
                parse_state = 1;
                continue;
            }

            if s == "INFORMATION" && parse_state < 2 {
                if parse_state != 1 {
                    return Err("gallery_metadata_sections".into());
                }
                parse_state = 2;
                continue;
            }

            // Skip empty line
            if parse_state < 2 && s.is_empty() {
                continue;
            }

            if parse_state == 0 {
                // Basic metadata
                let (key, value) = s.split_once(' ').ok_or("gallery_metadata_header")?;
                let split = [key, value];
                if matches!(key, "GID" | "FILECOUNT" | "MINXRES" | "DLHASH" | "TITLE") && !headers.insert(key) {
                    return Err("gallery_metadata_duplicate_header".into());
                }

                match split[0] {
                    "GID" => {
                        gid = split[1].parse()?;
                        debug!("GalleryDownloader: Parsed gid={}", gid);
                    }
                    "FILECOUNT" => {
                        filecount = split[1].parse()?;
                        debug!("GalleryDownloader: Parsed filecount={}", filecount);
                    }
                    "MINXRES" => {
                        if valid_resolution(split[1]) {
                            minxres = split[1].to_string();
                            debug!("GalleryDownloader: Parsed minxres={}", minxres);
                        } else {
                            return Err("gallery_metadata_resolution".into());
                        }
                    }
                    "DLHASH" => {
                        if Regex::new(r"^[a-f0-9]{40}$").unwrap().is_match(split[1]) {
                            dl_hash = split[1].to_string();
                        } else {
                            return Err("gallery_metadata_digest".into());
                        }
                    }
                    "TITLE" => {
                        if split[1].len() > 4096 || split[1].chars().any(char::is_control) {
                            return Err("gallery_metadata_title".into());
                        }
                        // Remove `*"\<>:|?/` and replace any space char to real space char, also trim start end space.
                        let pattern = Regex::new(r#"[*"\\<>:|?/]"#).unwrap().replace_all(split[1], "");
                        let pattern = Regex::new(r"\s+").unwrap().replace_all(&pattern, " ");
                        let pattern = Regex::new(r"(^\s+|\s+$)").unwrap().replace_all(&pattern, "").to_string();
                        title = pattern;
                        debug!("GalleryDownloader: Parsed title={}", title);
                    }
                    _ => (),
                }
            } else if parse_state == 1 {
                // Hash check
                hasher.update(b"\n");
                hasher.update(s.as_bytes());

                // File list
                let split = s.splitn(6, ' ').collect::<Vec<&str>>();
                if split.len() != 6 || gallery_files.len() >= 10000 {
                    return Err("gallery_metadata_file_row".into());
                }

                // entries are on the form: page fileindex xres sha1hash filetype filename
                let page: usize = split[0].parse()?;
                let fileindex: usize = split[1].parse()?;
                let xres = split[2].to_string();
                if page == 0 || page > 10000 || fileindex == 0 || !pages.insert(page) || !valid_resolution(&xres) {
                    return Err("gallery_metadata_file_identity".into());
                }

                // sha1hash can be "unknown" if the file has not been generated yet
                let sha1hash = if split[3] == "unknown" {
                    None
                } else {
                    Some(decode_to_array(split[3]).map_err(|_| "gallery_metadata_file_hash")?)
                };

                // the server checks that all filenames in the meta file are unique, and that none of them are reserved device filenames
                let filetype = split[4].to_string();
                if !allow_filetype.is_match(&filetype) {
                    return Err("gallery_metadata_file_type".into());
                }
                let filename = split[5].to_string();
                if !is_safe_path(&filename)
                    || !allow_filename.is_match(&filename)
                    || filename.to_ascii_lowercase().starts_with(".hath-manager")
                    || !paths.insert(format!("{filename}.{filetype}").to_ascii_lowercase())
                {
                    return Err("gallery_metadata_file_path".into());
                }

                gallery_files.push(Arc::new(GalleryFile {
                    page,
                    fileindex,
                    xres,
                    expected_sha1_hash: sha1hash,
                    filetype,
                    filename,
                }))
            } else {
                // Gallery info
                if information.len() + s.len() + 1 > 8192 || s.contains('\0') {
                    return Err("gallery_metadata_information".into());
                }
                information.push_str(s);
                information.push('\n');
            }
        }

        let hash = const_hex::encode(hasher.finish());
        if dl_hash.is_empty() || hash != dl_hash {
            return Err("gallery_metadata_digest_mismatch".into());
        }
        if gid <= 0
            || !(1..=10000).contains(&filecount)
            || gallery_files.len() != filecount
            || pages.iter().any(|page| *page > filecount)
            || minxres.is_empty()
            || title.is_empty()
            || parse_state != 2
            || headers.len() != 5
        {
            return Err("gallery_metadata_incomplete".into());
        }
        let xres_title = if minxres == "org" { String::new() } else { format!("-{minxres}x") };

        Ok(GalleryMeta {
            gid,
            filecount,
            minxres,
            xres_title,
            title,
            information,
            gallery_files,
            failures: Mutex::new(vec![]),
        })
    }
}

async fn download<P: AsRef<Path>>(
    reqwest: reqwest::Client,
    url: Url,
    path: P,
    hash: Option<[u8; 20]>,
    metrics: Arc<Metrics>,
    stop: &CancellationToken,
) -> Result<u64, BoxError> {
    let start = Instant::now();
    let path = path.as_ref().to_path_buf();
    let parent = path.parent().ok_or("gallery_image_directory")?.to_path_buf();
    let temporary =
        tokio::task::spawn_blocking(move || tempfile::Builder::new().prefix(".hath-manager.download-image-").tempfile_in(parent)).await??;
    let (file, temporary) = temporary.into_parts();
    let mut file = fs::File::from_std(file);
    let result: Result<u64, BoxError> = async {
        let response =
            tokio::select! { biased; _ = stop.cancelled() => return Err(cancelled()), result = reqwest.get(url).send() => result? };
        let response = response.error_for_status()?;
        if response.status() != reqwest::StatusCode::OK {
            return Err("gallery_unexpected_image_status".into());
        }
        let mut stream = response.bytes_stream();
        let mut hasher = digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY);
        let mut length = 0u64;
        loop {
            let next = tokio::select! { biased; _ = stop.cancelled() => return Err(cancelled()), result = stream.next() => result };
            let Some(bytes) = next else {
                break;
            };
            let bytes = bytes?;
            metrics.download_size.inc_by(bytes.len() as u64);
            #[cfg(all(feature="manager-integration",target_os="linux"))]
            metrics.observation.image_received_bytes(bytes.len() as u64);
            file.write_all(&bytes).await?;
            hasher.update(&bytes);
            length = length.checked_add(bytes.len() as u64).ok_or("gallery_image_limit")?;
        }
        if length == 0 {
            return Err("gallery_empty_image".into());
        }
        if let Some(expected) = hash {
            let hash = hasher.finish();
            if hash.as_ref() != expected {
                return Err(Box::new(Error::HashMismatch {
                    expected,
                    actual: hash.as_ref().try_into().unwrap(),
                }));
            }
        }
        file.flush().await?;
        file.sync_all().await?;
        Ok(length)
    }
    .await;
    // Also drain Tokio's pending file operation on cancellation/error before releasing the guard.
    let flushed = file.flush().await;
    drop(file);
    let length = result?;
    flushed?;
    if stop.is_cancelled() {
        return Err(cancelled());
    }
    tokio::task::spawn_blocking(move || {
        if let Ok(metadata) = std::fs::symlink_metadata(&path)
            && !metadata.is_file()
        {
            return Err(std::io::Error::other("gallery_image_not_regular"));
        }
        temporary.persist(&path).map_err(|error| error.error)?;
        sync_directory(path.parent().unwrap())
    })
    .await??;

    metrics.download_file_count.inc();
    #[cfg(all(feature="manager-integration",target_os="linux"))]
    metrics.observation.image_received_file();
    metrics.download_duration.observe(start.elapsed().as_secs_f64());

    Ok(length)
}

fn cancelled() -> BoxError {
    std::io::Error::new(std::io::ErrorKind::Interrupted, "gallery_cancelled").into()
}

// A slow image, file check or upstream RPC must not hide a live Gallery for minutes.
async fn observe_while<T>(future: impl std::future::Future<Output = T>, attempt: &mut GalleryObservation) -> T {
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    if let Some(attempt) = attempt {
        tokio::pin!(future);
        loop {
            tokio::select! { biased; result = &mut future => return result,
            _ = tokio::time::sleep(Duration::from_secs(30)) => attempt.active().await }
        }
    }
    #[cfg(not(all(feature = "manager-integration", target_os = "linux")))]
    let _ = attempt;
    future.await
}
fn sync_directory(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    std::fs::File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path; // Native managed handoff is Linux-only.
    Ok(())
}
async fn atomic_information(directory: &Path, information: String) -> std::io::Result<()> {
    use std::io::Write;
    let directory = directory.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut temporary = tempfile::Builder::new()
            .prefix(".hath-manager.download-info-")
            .tempfile_in(&directory)?;
        temporary.write_all(information.as_bytes())?;
        temporary.as_file().sync_all()?;
        temporary.persist(directory.join("galleryinfo.txt")).map_err(|error| error.error)?;
        sync_directory(&directory)
    })
    .await
    .map_err(|_| std::io::Error::other("gallery_info_worker"))?
}

#[cfg(all(feature = "manager-integration", target_os = "linux"))]
async fn handoff_pause(stop: &CancellationToken, warning: &mut Option<Instant>) -> bool {
    if warning.is_none_or(|time| time.elapsed() >= Duration::from_secs(60)) {
        warn!("Gallery handoff is not durable; pausing Gallery only until persistence recovers");
        *warning = Some(Instant::now());
    }
    tokio::select! { biased; _ = stop.cancelled() => false, _ = tokio::time::sleep(Duration::from_secs(1)) => true }
}

#[derive(Hash, Eq, PartialEq)]
struct GalleryFile {
    page: usize,
    fileindex: usize,
    xres: String,
    expected_sha1_hash: Option<[u8; 20]>,
    filetype: String,
    filename: String,
}

impl GalleryFile {
    async fn check_hash(&self, path: &Path) -> std::io::Result<bool> {
        let Some(expected) = self.expected_sha1_hash else {
            return Ok(false);
        };
        let metadata = match fs::symlink_metadata(path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(std::io::Error::other("gallery_file_not_regular"));
        }
        if metadata.len() == 0 {
            return Ok(false);
        }
        let mut options = fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        options.custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32);
        let mut file = options.open(path).await?;
        if !file.metadata().await?.is_file() {
            return Err(std::io::Error::other("gallery_file_not_regular"));
        }
        Ok(read_hash(&mut file).await?.as_ref() == expected)
    }
}

pub struct GalleryMeta {
    gid: i32,
    filecount: usize,
    minxres: String,
    xres_title: String,
    title: String,
    information: String,
    gallery_files: Vec<Arc<GalleryFile>>,
    failures: Mutex<Vec<String>>,
}

impl GalleryMeta {
    pub fn gid(&self) -> i32 {
        self.gid
    }

    pub fn minxres(&self) -> &str {
        self.minxres.as_ref()
    }
}

#[cfg(all(feature = "manager-integration", target_os = "linux"))]
impl GalleryView for GalleryMeta {
    fn gid(&self) -> i32 {
        self.gid
    }
    fn resolution(&self) -> &str {
        &self.minxres
    }
    fn title(&self) -> &str {
        &self.title
    }
    fn information(&self) -> &str {
        &self.information
    }
    fn file_count(&self) -> usize {
        self.filecount
    }
    fn files(&self) -> impl Iterator<Item = FileView<'_>> {
        self.gallery_files.iter().map(|file| FileView {
            page: file.page,
            filename: &file.filename,
            extension: &file.filetype,
            expected_sha1: file.expected_sha1_hash,
        })
    }
}

fn is_safe_path(name: &str) -> bool {
    let mut iter = Path::new(name).components();
    matches!((iter.next(), iter.next()), (Some(Component::Normal(_)), None))
}

fn valid_resolution(value: &str) -> bool {
    value == "org" || !value.is_empty() && value.len() <= 10 && value.bytes().all(|byte| byte.is_ascii_digit())
}

async fn read_hash(reader: &mut (impl tokio::io::AsyncRead + Unpin)) -> std::io::Result<digest::Digest> {
    // Keep nested/select-wrapped async futures small; do not embed a 64KiB array in them.
    let mut buffer = vec![0; 64 * 1024];
    let mut hasher = digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY);
    loop {
        let length = reader.read(&mut buffer).await?;
        if length == 0 {
            return Ok(hasher.finish());
        }
        hasher.update(&buffer[..length]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::ReadBuf;

    fn metadata(rows: &[&str]) -> Vec<String> {
        let mut digest = digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY);
        digest.update(b"test-key");
        for row in rows {
            digest.update(b"\n");
            digest.update(row.as_bytes());
        }
        vec![
            "GID 123".into(),
            "TITLE Example Gallery".into(),
            "MINXRES org".into(),
            format!("FILECOUNT {}", rows.len()),
            format!("DLHASH {}", const_hex::encode(digest.finish())),
            "FILELIST".into(),
        ]
        .into_iter()
        .chain(rows.iter().map(|row| row.to_string()))
        .chain(["INFORMATION".into(), "Example information".into()])
        .collect()
    }

    #[tokio::test]
    async fn downloader_future_stays_small_without_increasing_runtime_thread_stacks() {
        let client = Arc::new(RPCClient::new(12345, "test-key", false, 0, Some("127.0.0.1:9"), None));
        let downloader = GalleryDownloader::new(client, "unused", None, Arc::new(Metrics::new()), None);
        let future = downloader.run(CancellationToken::new()); // Never polled: no filesystem/network actions.
        assert!(std::mem::size_of_val(&future) <= 128 * 1024, "Gallery future grew to {} bytes", std::mem::size_of_val(&future));
    }
    #[test]
    fn parser_validates_file_identity_and_accepts_header_order_independently() {
        let parsed = GalleryMeta::parse(
            &metadata(&[
                "1 10 org unknown jpg 001",
                "2 11 1280 0123456789012345678901234567890123456789 png 002",
            ]),
            "test-key",
        )
        .unwrap();
        assert_eq!(parsed.gid, 123);
        assert_eq!(parsed.gallery_files.len(), 2);
        assert_eq!(parsed.minxres, "org");
        assert_eq!(parsed.xres_title, "");
        let invalid = [
            "1 10",
            "1 10 org bogus jpg 001",
            "0 10 org unknown jpg 001",
            "1 0 org unknown jpg 001",
            "1 10 bad unknown jpg 001",
            "1 10 org unknown jpg ..",
            "1 10 org unknown jpg .hath-manager-private",
        ];
        for row in invalid {
            assert!(GalleryMeta::parse(&metadata(&[row]), "test-key").is_err(), "{row}");
        }
        for rows in [
            ["1 10 org unknown jpg 001", "1 11 org unknown jpg 002"],
            ["1 10 org unknown jpg Name", "2 11 org unknown jpg name"],
        ] {
            assert!(GalleryMeta::parse(&metadata(&rows), "test-key").is_err());
        }
    }
    #[test]
    fn truncated_headers_bad_digest_count_and_sections_fail_without_panics() {
        let valid = metadata(&["1 10 org unknown jpg 001"]);
        for (index, replacement) in [
            (0, "GID"),
            (0, "GID 0"),
            (3, "FILECOUNT 2"),
            (4, "DLHASH invalid"),
            (5, "INFORMATION"),
            (7, "FILELIST"),
        ] {
            let mut raw = valid.clone();
            raw[index] = replacement.into();
            assert!(GalleryMeta::parse(&raw, "test-key").is_err());
        }
        let mut duplicate = valid.clone();
        duplicate.insert(1, "GID 123".into());
        assert!(GalleryMeta::parse(&duplicate, "test-key").is_err());
        assert!(GalleryMeta::parse(&valid, "different-key").is_err());
        let mut oversized = valid.clone();
        oversized.push("x".repeat(8193));
        assert!(GalleryMeta::parse(&oversized, "test-key").is_err());
    }

    struct BrokenReader(usize);
    impl tokio::io::AsyncRead for BrokenReader {
        fn poll_read(mut self: Pin<&mut Self>, _: &mut Context<'_>, _: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            self.0 += 1;
            assert_eq!(self.0, 1, "an I/O error must not restart the hashing loop");
            Poll::Ready(Err(io::Error::other("injected read failure")))
        }
    }
    #[tokio::test]
    async fn hashing_propagates_io_error_and_unknown_hash_does_not_trust_an_existing_file() {
        assert!(read_hash(&mut BrokenReader(0)).await.is_err());
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("001.jpg");
        fs::write(&path, b"partial").await.unwrap();
        let meta = GalleryMeta::parse(&metadata(&["1 10 org unknown jpg 001"]), "test-key").unwrap();
        assert!(!meta.gallery_files[0].check_hash(&path).await.unwrap());
        let expected = const_hex::encode(digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, b"partial"));
        let meta = GalleryMeta::parse(&metadata(&[&format!("1 10 org {expected} jpg 001")]), "test-key").unwrap();
        assert!(meta.gallery_files[0].check_hash(&path).await.unwrap());
    }

    async fn image_server(status: u16, body: &'static [u8], declared: usize) -> (Url, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/image", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await.unwrap());
                assert!(request.len() < 8192);
            }
            stream
                .write_all(format!("HTTP/1.1 {status} Reply\r\nContent-Length: {declared}\r\nConnection: close\r\n\r\n").as_bytes())
                .await
                .unwrap();
            stream.write_all(body).await.unwrap();
        });
        (url, server)
    }

    #[tokio::test]
    async fn image_commit_is_atomic_and_failed_or_partial_responses_never_replace_final_file() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("001.jpg");
        let client = util::create_http_client(Duration::from_secs(3), None, None);
        let stop = CancellationToken::new();
        let metrics = Arc::new(Metrics::new());
        let expected = digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, b"image-one")
            .as_ref()
            .try_into()
            .unwrap();
        let (url, server) = image_server(200, b"image-one", 9).await;
        assert_eq!(
            download(client.clone(), url, &path, Some(expected), metrics.clone(), &stop)
                .await
                .unwrap(),
            9
        );
        server.await.unwrap();
        assert_eq!(fs::read(&path).await.unwrap(), b"image-one");
        assert_eq!(metrics.download_file_count.get(), 1);
        for (status, declared, hash) in [(200, 9, Some([0; 20])), (200, 100, None), (206, 9, None), (200, 0, None)] {
            let body: &'static [u8] = if declared == 0 { b"" } else { b"image-two" };
            let (url, server) = image_server(status, body, declared).await;
            assert!(download(client.clone(), url, &path, hash, metrics.clone(), &stop).await.is_err());
            server.await.unwrap();
            assert_eq!(fs::read(&path).await.unwrap(), b"image-one");
            assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
        }
        assert_eq!(metrics.download_file_count.get(), 1);
    }

    #[tokio::test]
    async fn cancellation_waits_for_writer_cleanup_without_publishing_partial_file() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/image", listener.local_addr().unwrap())).unwrap();
        let (started, received) = tokio::sync::oneshot::channel();
        let stop = CancellationToken::new();
        let server_stop = stop.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await.unwrap());
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\nConnection: close\r\n\r\npartial")
                .await
                .unwrap();
            started.send(()).unwrap();
            server_stop.cancelled().await;
        });
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("001.jpg");
        let worker_path = path.clone();
        let worker_stop = stop.clone();
        let mut writers = JoinSet::new();
        writers.spawn(async move {
            download(
                util::create_http_client(Duration::from_secs(3), None, None),
                url,
                worker_path,
                None,
                Arc::new(Metrics::new()),
                &worker_stop,
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), received).await.unwrap().unwrap();
        assert!(!path.exists());
        stop.cancel();
        let result = tokio::time::timeout(Duration::from_secs(2), writers.join_next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(result.is_err());
        server.await.unwrap();
        assert!(!path.exists());
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    mod managed {
        use super::*;
        use axum::{Router, extract::State, http::Uri, routing::get};
        use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

        struct Mock {
            address: String,
            root: PathBuf,
            spool: PathBuf,
            rows: Vec<String>,
            advanced: AtomicUsize,
            requests: Mutex<Vec<Instant>>,
            active: AtomicUsize,
            peak: AtomicUsize,
            started: AtomicUsize,
            release: tokio::sync::watch::Sender<bool>,
            stop: CancellationToken,
        }
        async fn control(State(mock): State<Arc<Mock>>, uri: Uri) -> String {
            let url = Url::parse(&format!("http://localhost{uri}")).unwrap();
            let query: HashMap<_, _> = url
                .query_pairs()
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect();
            match query.get("act").map(String::as_str).unwrap() {
                "fetchqueue" if query["add"].is_empty() => mock.rows.join("\n"),
                "fetchqueue" => {
                    assert_eq!(query["add"], "123;org");
                    let directory = std::fs::read_dir(mock.root.join("Example Gallery [123]"))
                        .unwrap()
                        .next()
                        .unwrap()
                        .unwrap()
                        .path();
                    assert!(directory.join(".hath-manager.json").is_file());
                    let journal: serde_json::Value =
                        serde_json::from_slice(&std::fs::read(directory.join(".hath-manager.download-handoff.json")).unwrap()).unwrap();
                    assert_eq!(journal["terminal"]["ready"], true);
                    let persisted = std::fs::read_dir(mock.spool.join("11111111-1111-4111-8111-111111111111"))
                        .unwrap()
                        .any(|entry| {
                            let value: serde_json::Value = serde_json::from_slice(&std::fs::read(entry.unwrap().path()).unwrap()).unwrap();
                            value["eventType"] == "gallery.download.completed"
                        });
                    assert!(persisted, "queue may only advance after the event is durable");
                    mock.advanced.fetch_add(1, Relaxed);
                    "NO_PENDING_DOWNLOADS".into()
                }
                "dlfetch" => {
                    mock.requests.lock().push(Instant::now());
                    let page = query["add"].split(';').nth(1).unwrap();
                    let body = format!("http://{}/image/{page}", mock.address);
                    let hash =
                        const_hex::encode(digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, format!("0-test-key\n{body}").as_bytes()));
                    format!("OK-0-{hash}\n{body}")
                }
                "client_stop" => format!("OK-0-{}", const_hex::encode(digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, b"0-test-key"))),
                other => panic!("unexpected mock action {other}"),
            }
        }
        async fn image(State(mock): State<Arc<Mock>>) -> Vec<u8> {
            let active = mock.active.fetch_add(1, Relaxed) + 1;
            mock.peak.fetch_max(active, Relaxed);
            if mock.started.fetch_add(1, Relaxed) + 1 == 4 {
                mock.release.send_replace(true);
            }
            let mut receiver = mock.release.subscribe();
            tokio::select! { biased; _ = mock.stop.cancelled() => (), _ = receiver.wait_for(|released|*released) => () }
            mock.active.fetch_sub(1, Relaxed);
            b"image-one".to_vec()
        }
        async fn setup(root: &Path, spool: PathBuf) -> (Arc<Mock>, tokio::task::JoinHandle<()>, GalleryDownloader) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let directory = root.join("download");
            std::fs::create_dir(&directory).unwrap();
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
            let mock = Arc::new(Mock {
                address: address.clone(),
                root: directory.clone(),
                spool,
                rows: metadata(&[
                    "1 10 org unknown jpg 001",
                    "2 11 org unknown jpg 002",
                    "3 12 org unknown jpg 003",
                    "4 13 org unknown jpg 004",
                ]),
                advanced: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
                active: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                started: AtomicUsize::new(0),
                release: tokio::sync::watch::channel(false).0,
                stop: CancellationToken::new(),
            });
            let router = Router::new()
                .route("/16/{endpoint}", get(control))
                .route("/image/{page}", get(image))
                .route("/health", get(|| async { "ok" }))
                .with_state(mock.clone());
            let server_stop = mock.stop.clone();
            let server = tokio::spawn(async move {
                let mut connections = JoinSet::new();
                loop {
                    let accepted =
                        tokio::select! { biased; _ = server_stop.cancelled() => break, result = listener.accept() => result.unwrap() };
                    let service = hyper_util::service::TowerToHyperService::new(router.clone());
                    connections.spawn(async move {
                        hyper::server::conn::http1::Builder::new()
                            .keep_alive(false)
                            .serve_connection(hyper_util::rt::TokioIo::new(accepted.0), service)
                            .await
                            .unwrap();
                    });
                }
                while let Some(result) = connections.join_next().await {
                    result.unwrap();
                }
            });
            let client = Arc::new(RPCClient::new(12345, "test-key", false, 0, Some(&address), None));
            client.set_running_for_test();
            (mock, server, GalleryDownloader::new(client, directory, None, Arc::new(Metrics::new()), None))
        }

        #[tokio::test]
        async fn actual_downloader_preserves_four_concurrency_and_pacing_and_only_advances_after_durable_handoff() {
            let root = tempfile::tempdir().unwrap();
            let (runtime, _listener, spool) = crate::manager_integration::test_runtime(root.path(), 4096).await;
            let (mock, server, downloader) = setup(root.path(), spool).await;
            let client = downloader.client.clone();
            let stop = CancellationToken::new();
            let result =
                tokio::time::timeout(Duration::from_secs(12), downloader.with_integration(runtime.handle()).run(stop.clone())).await;
            stop.cancel();
            client.shutdown().await;
            mock.stop.cancel();
            server.await.unwrap();
            assert!(result.is_ok());
            assert_eq!(mock.peak.load(Relaxed), 4);
            assert_eq!(mock.active.load(Relaxed), 0);
            assert_eq!(mock.advanced.load(Relaxed), 1);
            {
                let times = mock.requests.lock();
                assert_eq!(times.len(), 4);
                assert!(
                    times
                        .windows(2)
                        .all(|pair| pair[1].duration_since(pair[0]) >= Duration::from_millis(800))
                );
            }
            runtime.shutdown().await.unwrap();
        }

        #[tokio::test]
        async fn exhausted_handoff_budget_pauses_gallery_without_blocking_independent_http_or_acknowledging_queue() {
            let root = tempfile::tempdir().unwrap();
            let (runtime, _listener, spool) = crate::manager_integration::test_runtime(root.path(), 1).await;
            let run_spool = spool.join("11111111-1111-4111-8111-111111111111");
            let (mock, server, downloader) = setup(root.path(), spool).await;
            let client = downloader.client.clone();
            let stop = CancellationToken::new();
            let worker_stop = stop.clone();
            let downloader = downloader.with_integration(runtime.handle());
            let task = tokio::spawn(async move { downloader.run(worker_stop).await });
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if std::fs::read_dir(&run_spool).is_ok_and(|entries| entries.count() == 1) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let response = util::create_http_client(Duration::from_secs(3), None, None)
                .get(format!("http://{}/health", mock.address))
                .send()
                .await
                .unwrap();
            assert_eq!(response.text().await.unwrap(), "ok");
            assert!(mock.requests.lock().is_empty());
            assert_eq!(mock.advanced.load(Relaxed), 0);
            assert!(!task.is_finished());
            stop.cancel();
            tokio::time::timeout(Duration::from_secs(2), task).await.unwrap().unwrap();
            client.shutdown().await;
            mock.stop.cancel();
            server.await.unwrap();
            runtime.shutdown().await.unwrap();
        }
    }
}

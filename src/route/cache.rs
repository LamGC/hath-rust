use std::{
    io::SeekFrom,
    ops::RangeInclusive,
    sync::Arc,
    time::{Duration, Instant},
};

use async_stream::stream;
use aws_lc_rs::digest;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{
            ACCEPT_RANGES, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_NONE_MATCH, IF_RANGE,
            RANGE,
        },
    },
    response::{IntoResponse, Response},
};
use futures::StreamExt;
use http_range_header::parse_range_header;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::watch,
    time::timeout,
};

use crate::{
    AppState,
    cache_manager::{CacheFileInfo, CacheFileResult},
    metrics::{LABEL_CACHE_DOWNLOAD, LABEL_CACHE_FETCH_URL},
    route::{forbidden, not_found, parse_additional},
    server::util::TruncateBody,
    util::{create_http_client, string_to_hash},
};

const TTL: RangeInclusive<i64> = -900..=900; // Token TTL 15 minutes
const CACHE_HEADER: HeaderValue = HeaderValue::from_static("public, max-age=31536000, immutable");
const DEFAULT_CD: HeaderValue = HeaderValue::from_static("inline");

pub(super) async fn hath(
    Path((file_id, additional, file_name)): Path<(String, String, String)>,
    headers: HeaderMap,
    data: State<Arc<AppState>>,
) -> impl IntoResponse {
    let additional = parse_additional(&additional);
    let mut keystamp = additional.get("keystamp").unwrap_or(&"").split('-');
    let file_index = additional.get("fileindex").unwrap_or(&"");
    let xres = additional.get("xres").unwrap_or(&"");

    // keystamp check
    let time = keystamp.next().unwrap_or_default();
    let hash = keystamp.next().unwrap_or_default();
    let time_diff = &(data.rpc.get_timestemp() - time.parse::<i64>().unwrap_or_default());
    let hash_string = format!("{}-{}-{}-hotlinkthis", time, file_id, data.rpc.key());
    if time.is_empty() || hash.is_empty() || !TTL.contains(time_diff) || !string_to_hash(hash_string).starts_with(hash) {
        return forbidden();
    };

    // Parse file info
    let info = match CacheFileInfo::from_file_id(&file_id) {
        Some(info) if info.size() > 0 => info,
        _ => return not_found(),
    };
    let file_size = info.size() as u64;
    let content_type = HeaderValue::from_maybe_shared(info.mime_type().to_string()).unwrap();
    let content_disposition = HeaderValue::from_maybe_shared(format!("inline; filename=\"{file_name}\"")).unwrap_or(DEFAULT_CD);

    // ETag
    // Browser only send range request if have etag
    let etag = {
        let mut builder = String::with_capacity(10);
        builder.push('"');
        builder.push_str(&const_hex::encode(&info.hash()[..4])); // First 8 hex
        builder.push('"');
        builder
    };
    // Handle If-None-Match
    if headers.get(IF_NONE_MATCH).is_some_and(|h| h.to_str().is_ok_and(|s| s == etag)) {
        // 304 Not Modified
        return Response::builder()
            .header(CACHE_CONTROL, CACHE_HEADER)
            .header(CONTENT_LENGTH, file_size) // rfc9110 section 8.6
            .header(ETAG, etag)
            .status(StatusCode::NOT_MODIFIED)
            .body(Body::empty())
            .unwrap();
    }

    // Range request
    let mut range = headers
        .get(RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|h| parse_range_header(h).ok())
        .and_then(|r| r.validate(file_size).ok())
        .filter(|r| r.len() == 1)
        .and_then(|r| r.into_iter().next());
    // Check If-Range
    if headers.get(IF_RANGE).is_some_and(|h| h.to_str().is_ok_and(|s| s != etag)) {
        range.take(); // etag not match, ignore range request
    }

    // Build response
    let response = Response::builder()
        .header(CACHE_CONTROL, CACHE_HEADER)
        .header(CONTENT_DISPOSITION, content_disposition)
        .header(CONTENT_TYPE, content_type)
        .header(ETAG, etag);

    // Check cache hit
    let file_stream = match data.cache_manager.get_file(&info, range.as_ref().map(|r| *r.start())).await {
        CacheFileResult::Full(stream) => {
            if range.as_ref().is_some_and(|r| *r.start() != 0) {
                // File seek fail, reject range request
                range.take();
            }
            Some(stream)
        }
        CacheFileResult::Partial(stream) => Some(stream),
        CacheFileResult::NotFound => None,
    };

    if let Some(stream) = file_stream {
        return image_response(
            if let Some(r) = range {
                let content_length = r.end() - r.start() + 1;
                let content_range = format!("bytes {}-{}/{}", r.start(), r.end(), file_size);
                response
                    .header(ACCEPT_RANGES, "bytes")
                    .header(CONTENT_LENGTH, content_length)
                    .header(CONTENT_RANGE, content_range)
                    .status(StatusCode::PARTIAL_CONTENT)
                    .body(Body::new(TruncateBody::new(Body::from_stream(stream), content_length)))
                    .unwrap()
            } else {
                response
                    .header(ACCEPT_RANGES, "bytes")
                    .header(CONTENT_LENGTH, file_size)
                    .body(Body::new(TruncateBody::new(Body::from_stream(stream), file_size)))
                    .unwrap()
            },
            true,
            file_size,
        );
    }

    // Cache miss: one owned writer per hash, even when the first visitor leaves.
    if data.cache_manager.io_stopped() {
        return not_found();
    }
    let (temp_tx, temp_rx) = watch::channel(None);
    let tx = Arc::new(watch::channel(0).0);
    let state = {
        let mut states = data.download_state.lock();
        let state = states.get(&info.hash()).cloned();
        if state.is_none() {
            states.insert(info.hash(), (temp_rx, tx.clone()));
        }
        state
    };
    let (temp_path, mut rx) = if let Some((mut tempfile, progress)) = state {
        let path = match timeout(Duration::from_secs(30), tempfile.wait_for(Option::is_some)).await {
            Ok(Ok(path)) => path.as_ref().unwrap().clone(),
            _ => return not_found(),
        };
        (path, progress.subscribe())
    } else {
        let owner = data.clone();
        let expected = tx.clone();
        let state_guard = scopeguard::guard(info.hash(), move |hash| {
            let mut states = owner.download_state.lock();
            if states.get(&hash).is_some_and(|(_, current)| Arc::ptr_eq(current, &expected)) {
                states.remove(&hash);
            }
        });
        let temp_path = match data.cache_manager.create_temp_file().await {
            Ok(path) => Arc::new(path),
            Err(_) => return not_found(),
        };
        temp_tx.send_replace(Some(temp_path.clone()));
        let fetch_time = Instant::now();
        let sources = match data.rpc.sr_fetch(file_index, xres, &file_id).await {
            Some(sources) if !sources.is_empty() => sources,
            _ => return not_found(),
        };
        data.metrics
            .cache_received_duration
            .get_or_create(&LABEL_CACHE_FETCH_URL)
            .observe(fetch_time.elapsed().as_secs_f64());
        let worker_data = data.clone();
        let info2 = info.clone();
        let path = temp_path.clone();
        let progress_tx = tx.clone();
        let stop = data.cache_manager.io_stop();
        if !data.cache_manager.spawn_io(async move {
            let _guard = state_guard;
            #[cfg(all(feature = "manager-integration", target_os = "linux"))]
            let mut observation = worker_data
                .metrics
                .observation
                .operation(crate::manager_integration::Operation::ProxyUpstream);
            let result = async {
                let download_time = Instant::now();
                let mut progress = 0u64;
                let mut hasher = digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY);
                let mut client = worker_data.reqwest.clone();
                let mut use_proxy = worker_data.has_proxy;
                'retry: for retry in 0..3 {
                    if stop.is_cancelled() {
                        return Err(());
                    }
                    let mut file = OpenOptions::new().write(true).open(&*path).await.map_err(|_| ())?;
                    file.seek(SeekFrom::Start(progress)).await.map_err(|_| ())?;
                    let source = &sources[retry % sources.len()];
                    let request = tokio::select! {biased;_=stop.cancelled()=>return Err(()),result=client.get(source).send()=>result};
                    let response = match request.and_then(|response| response.error_for_status()) {
                        Ok(response) if response.status() == StatusCode::OK => response,
                        Ok(_) => continue,
                        Err(error) => {
                            if use_proxy && (error.is_connect() || retry == 1) {
                                client = create_http_client(Duration::from_secs(30), None, worker_data.local_addr);
                                use_proxy = false;
                            }
                            continue;
                        }
                    };
                    let mut stream = response.bytes_stream();
                    let mut downloaded = 0u64;
                    while let Some(chunk) = tokio::select! {biased;_=stop.cancelled()=>return Err(()),chunk=stream.next()=>chunk} {
                        let chunk = match chunk {
                            Ok(chunk) => chunk,
                            Err(_) => continue 'retry,
                        };
                        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
                        worker_data.metrics.observation.image_received_bytes(chunk.len() as u64);
                        let before = downloaded;
                        downloaded = downloaded.checked_add(chunk.len() as u64).ok_or(())?;
                        if downloaded > file_size {
                            return Err(());
                        }
                        if downloaded <= progress {
                            continue;
                        }
                        let skip = progress.saturating_sub(before) as usize;
                        let bytes = &chunk[skip..];
                        file.write_all(bytes).await.map_err(|_| ())?;
                        hasher.update(bytes);
                        progress += bytes.len() as u64;
                        worker_data.metrics.cache_received_size.inc_by(bytes.len() as u64);
                        // Never release the final byte before exact-length/hash/fsync validation.
                        progress_tx.send_replace(progress.min(file_size.saturating_sub(1)));
                    }
                    if progress != file_size {
                        continue;
                    }
                    file.flush().await.map_err(|_| ())?;
                    file.sync_all().await.map_err(|_| ())?;
                    drop(file);
                    if hasher.finish().as_ref() != info2.hash() {
                        return Err(());
                    }
                    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
                    worker_data.metrics.observation.image_received_file();
                    progress_tx.send_replace(progress);
                    // Import while readers retain the original immutable temporary inode.
                    worker_data.cache_manager.import_cache(&info2, &path).await;
                    worker_data.metrics.cache_received.inc();
                    worker_data
                        .metrics
                        .cache_received_duration
                        .get_or_create(&LABEL_CACHE_DOWNLOAD)
                        .observe(download_time.elapsed().as_secs_f64());
                    return Ok(());
                }
                Err(())
            }
            .await;
            #[cfg(all(feature = "manager-integration", target_os = "linux"))]
            if !stop.is_cancelled() {
                observation.result(result.is_ok());
            }
            if result.is_ok() {
                tokio::select! {biased;_=stop.cancelled()=>(),_=progress_tx.closed()=>()}
            }
        }) {
            return not_found();
        }
        (temp_path, tx.subscribe())
    };
    if *rx.borrow() == 0
        && !timeout(Duration::from_secs(30), rx.changed())
            .await
            .is_ok_and(|result| result.is_ok())
    {
        return not_found();
    }
    let result = response
        .header(CONTENT_LENGTH, file_size)
        .body(Body::from_stream(stream! {
            let mut file=match File::open(temp_path.as_ref()).await{Ok(file)=>file,Err(error)=>{yield Err(error);return;}};
            let mut offset=0u64;
            loop{
                let available=(*rx.borrow()).min(file_size);
                if available>offset{
                    let mut buffer=vec![0u8;(available-offset).min(64*1024) as usize];
                    match file.read(&mut buffer).await{
                        Ok(0)=>{yield Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof,"proxy_short_file"));return;},
                        Ok(size)=>{offset+=size as u64;buffer.truncate(size);yield Ok(bytes::Bytes::from(buffer));},
                        Err(error)=>{yield Err(error);return;}
                    }
                    if offset==file_size{return;}
                }else if !timeout(Duration::from_secs(30),rx.changed()).await.is_ok_and(|result|result.is_ok()){
                    yield Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof,"proxy_upstream_incomplete"));return;
                }
            }
        }))
        .unwrap();
    image_response(result, false, file_size)
}

fn image_response(response: Response, local: bool, bytes: u64) -> Response {
    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
    {
        let mut response = response;
        response.extensions_mut().insert(crate::manager_integration::ImageResponse {
            source: if local {
                crate::manager_integration::ImageSource::Local
            } else {
                crate::manager_integration::ImageSource::Proxy
            },
            bytes,
        });
        response
    }
    #[cfg(not(all(feature = "manager-integration", target_os = "linux")))]
    {
        let _ = (local, bytes);
        response
    }
}

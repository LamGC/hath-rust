//! Producer-view accounting, not an assertion of remote receipt or TCP ACK.
use axum::http::Method;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Instant,
};
const ROUTES: usize = 7;
const METHODS: usize = 3;
const STATUS: usize = 6;
const OUTCOMES: usize = 4;
pub(super) const ROWS: usize = ROUTES * METHODS * STATUS * OUTCOMES;
pub(super) struct Counters {
    rows: [[AtomicU64; 3]; ROWS],
    images: [[AtomicU64; 3]; 12],
    sent: AtomicU64,
    files_sent: AtomicU64,
    received: AtomicU64,
    files_received: AtomicU64,
}
impl Counters {
    pub(super) fn new() -> Self {
        Self {
            rows: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            images: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            sent: AtomicU64::new(0),
            files_sent: AtomicU64::new(0),
            received: AtomicU64::new(0),
            files_received: AtomicU64::new(0),
        }
    }
    fn record(&self, key: Key, outcome: usize, bytes: u64, micros: u64) {
        let row = &self.rows[((key.route * METHODS + key.method) * STATUS + key.status) * OUTCOMES + outcome];
        add(&row[0], 1);
        add(&row[1], bytes);
        add(&row[2], micros);
    }
    pub(super) fn snapshot(&self) -> Value {
        let mut output = Vec::new();
        for (index, row) in self.rows.iter().enumerate() {
            let count = row[0].load(Relaxed);
            if count == 0 {
                continue;
            }
            let outcome = index % OUTCOMES;
            let status = index / OUTCOMES % STATUS;
            let method = index / OUTCOMES / STATUS % METHODS;
            let route = index / OUTCOMES / STATUS / METHODS;
            output.push([
                route as u64,
                method as u64,
                status as u64,
                outcome as u64,
                count,
                row[1].load(Relaxed),
                row[2].load(Relaxed),
            ]);
        }
        let mut images = Vec::new();
        for (index, row) in self.images.iter().enumerate() {
            let count = row[0].load(Relaxed);
            if count > 0 {
                images.push([
                    index as u64 / 6,
                    index as u64 / 3 % 2,
                    index as u64 % 3 + 1,
                    count,
                    row[1].load(Relaxed),
                    row[2].load(Relaxed),
                ]);
            }
        }
        json!({"nativeHttpVersion":1,"nativeHttp":output,"nativeImageResponses":images,"bytesSent":self.sent.load(Relaxed),"filesSent":self.files_sent.load(Relaxed),"bytesReceived":self.received.load(Relaxed),"filesReceived":self.files_received.load(Relaxed)})
    }
    pub(super) fn received(&self, bytes: u64) {
        add(&self.received, bytes);
    }
    pub(super) fn received_file(&self) {
        add(&self.files_received, 1);
    }
}
#[derive(Clone, Copy, Default)]
struct Key {
    route: usize,
    method: usize,
    status: usize,
}
#[derive(Clone, Copy)]
pub(crate) enum ImageSource {
    Local,
    Proxy,
}
#[derive(Clone, Copy)]
pub(crate) struct ImageResponse {
    pub source: ImageSource,
    pub bytes: u64,
}
pub(crate) struct HttpObservation {
    counters: Option<Arc<Counters>>,
    key: Key,
    since: Option<Instant>,
    bytes: u64,
    expected: Option<u64>,
    image: Option<ImageResponse>,
    status: u16,
}
impl HttpObservation {
    pub(super) fn new(counters: Option<Arc<Counters>>, method: &Method, path: &str) -> Self {
        let route = if path.starts_with("/h/") {
            0
        } else if path.starts_with("/servercmd/") {
            1
        } else if path.starts_with("/t/") {
            2
        } else if path == "/favicon.ico" {
            3
        } else if path == "/robots.txt" {
            4
        } else if path == "/metrics" {
            5
        } else {
            6
        };
        let method = if method == Method::GET {
            0
        } else if method == Method::HEAD {
            1
        } else {
            2
        };
        let key = Key { route, method, status: 0 };
        if let Some(counters) = &counters {
            counters.record(key, 0, 0, 0);
        }
        let since = counters.as_ref().map(|_| Instant::now());
        Self {
            counters,
            key,
            since,
            bytes: 0,
            expected: None,
            image: None,
            status: 0,
        }
    }
    pub(crate) fn headers(&mut self, status: u16, expected: Option<u64>, image: Option<ImageResponse>, ended: bool) {
        self.key.status = if (100..600).contains(&status) { (status / 100) as usize } else { 0 };
        self.status = status;
        self.image = image;
        let suppressed = self.key.method == 1 || status < 200 || status == 204 || status == 304;
        self.expected = if suppressed {
            Some(0)
        } else {
            expected.or_else(|| image.filter(|_| status == 200).map(|image| image.bytes))
        };
        if suppressed || ended {
            self.end();
        }
    }
    pub(crate) fn data(&mut self, bytes: u64) {
        if self.counters.is_none() {
            return;
        }
        self.bytes = self.bytes.saturating_add(bytes);
        if self.image.is_some() && self.key.route == 0 && self.key.method == 0 && matches!(self.status, 200 | 206) {
            add(&self.counters.as_ref().unwrap().sent, bytes);
        }
        // Hyper may stop polling as soon as Content-Length is fulfilled. This is
        // the end of the selected HTTP view, not proof of remote receipt.
        if self.expected.is_some_and(|expected| self.bytes >= expected) {
            self.end();
        }
    }
    pub(crate) fn end(&mut self) {
        if self.expected.is_some_and(|expected| expected != self.bytes) {
            self.finish(2);
            return;
        }
        if self.key.route == 0
            && self.key.method == 0
            && self.status == 200
            && self.image.is_some_and(|image| image.bytes > 0 && image.bytes == self.bytes)
            && let Some(counters) = &self.counters
        {
            add(&counters.files_sent, 1);
        }
        self.finish(1);
    }
    pub(crate) fn failed(&mut self) {
        self.finish(2);
    }
    fn finish(&mut self, outcome: usize) {
        if let Some(counters) = self.counters.take() {
            let micros = self.since.take().unwrap().elapsed().as_micros().min(u64::MAX as u128) as u64;
            counters.record(self.key, outcome, self.bytes, micros);
            if self.key.route == 0
                && self.key.method == 0
                && matches!(self.status, 200 | 206)
                && let Some(image) = self.image
            {
                let index = ((image.source as usize) * 2 + usize::from(self.status == 206)) * 3 + outcome - 1;
                let row = &counters.images[index];
                add(&row[0], 1);
                add(&row[1], self.bytes);
                add(&row[2], micros);
            }
        }
    }
}
impl Drop for HttpObservation {
    fn drop(&mut self) {
        self.finish(3);
    }
}
fn add(counter: &AtomicU64, value: u64) {
    let _ = counter.try_update(Relaxed, Relaxed, |previous| Some(previous.saturating_add(value)));
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn completed_failed_cancelled_and_body_suppression_have_distinct_meaning() {
        let counters = Arc::new(Counters::new());
        let image = Some(ImageResponse {
            source: ImageSource::Local,
            bytes: 9,
        });
        {
            let mut guard = HttpObservation::new(Some(counters.clone()), &Method::GET, "/h/private/secret");
            guard.headers(200, Some(9), image, false);
            guard.data(9);
            guard.end();
        }
        {
            let mut guard = HttpObservation::new(Some(counters.clone()), &Method::GET, "/h/other");
            guard.headers(200, Some(9), image, false);
            guard.data(3);
        }
        {
            let mut guard = HttpObservation::new(Some(counters.clone()), &Method::GET, "/h/other");
            guard.headers(200, Some(9), image, false);
            guard.data(2);
            guard.end();
        }
        for (method, status) in [(Method::HEAD, 200), (Method::GET, 304)] {
            let mut guard = HttpObservation::new(Some(counters.clone()), &method, "/h/other");
            guard.headers(status, Some(9), image, true);
        }
        {
            let mut guard = HttpObservation::new(Some(counters.clone()), &Method::GET, "/h/other");
            guard.headers(206, Some(3), image, false);
            guard.data(3);
            guard.end();
        }
        let payload = counters.snapshot();
        assert_eq!(payload["filesSent"], 1);
        assert_eq!(payload["bytesSent"], 17);
        assert!(!payload.to_string().contains("secret"));
        let outcomes: Vec<_> = payload["nativeHttp"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row[3].as_u64().unwrap())
            .collect();
        assert!(outcomes.contains(&2) && outcomes.contains(&3));
    }
    #[test]
    fn dense_http_and_operations_fit_the_snapshot_budget() {
        let counters = Counters::new();
        for row in &counters.rows {
            for value in row {
                value.store(u64::MAX, Relaxed);
            }
        }
        assert_eq!(counters.snapshot()["nativeHttp"].as_array().unwrap().len(), ROWS);
        assert!(serde_json::to_vec(&counters.snapshot()).unwrap().len() < 40 * 1024);
    }
}

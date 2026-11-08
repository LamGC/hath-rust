#[cfg(all(feature = "manager-integration", target_os = "linux"))]
use crate::manager_integration::{HttpObservation, ImageResponse};
use crate::{metrics::Metrics, server::ClientAddr};
use axum::{
    body::{Body, HttpBody},
    extract::Request,
    http::{Method, Version, header::CONTENT_LENGTH},
    response::Response,
};
use futures::future::BoxFuture;
use http_body::{Frame, SizeHint};
use log::info;
use pin_project_lite::pin_project;
use std::{
    cmp::max,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};
use tower::{Layer, Service};
#[cfg(not(all(feature = "manager-integration", target_os = "linux")))]
type HttpObservation = ();

#[derive(Clone)]
pub(super) struct Logger {
    metrics: Arc<Metrics>,
}
impl Logger {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }
}
impl<S> Layer<S> for Logger {
    type Service = LoggerMiddleware<S>;
    fn layer(&self, service: S) -> Self::Service {
        LoggerMiddleware {
            metrics: self.metrics.clone(),
            service,
        }
    }
}
#[derive(Clone)]
pub(super) struct LoggerMiddleware<S> {
    metrics: Arc<Metrics>,
    service: S,
}
impl<S> Service<Request> for LoggerMiddleware<S>
where
    S: Service<Request, Response = Response> + Send + 'static,
    S::Future: Send + 'static,
{
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
    type Response = Response<LoggerFinalizer>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }
    fn call(&mut self, req: Request) -> Self::Future {
        let start = Instant::now();
        let count = self.metrics.cache_sent.inc() + 1;
        let version = req.version();
        let is_head = req.method() == Method::HEAD;
        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
        let managed = self.metrics.observation.enabled();
        #[cfg(not(all(feature = "manager-integration", target_os = "linux")))]
        let managed = false;
        // Managed request paths never allocate/log IPs or raw URI/query values.
        let ip = (!managed).then(|| {
            req.extensions()
                .get::<ClientAddr>()
                .map(|i| i.ip().to_string())
                .unwrap_or_else(|| "-".into())
        });
        let request = (!managed).then(|| format!("{} {} {:?}", req.method(), req.uri(), version));
        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
        let mut native = self.metrics.observation.http(req.method(), req.uri().path());
        #[cfg(not(all(feature = "manager-integration", target_os = "linux")))]
        let native = ();
        let future = self.service.call(req);
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let res = match future.await {
                Ok(res) => res,
                Err(error) => {
                    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
                    native.failed();
                    return Err(error);
                }
            };
            let code = res.status().as_u16();
            let length = res
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|header| header.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            #[cfg(all(feature = "manager-integration", target_os = "linux"))]
            native.headers(code, length, res.extensions().get::<ImageResponse>().copied(), res.body().is_end_stream());
            if let (Some(ip), Some(request)) = (&ip, &request) {
                info!(
                    "{{{}/{:16} Code={} Byte={:<8} {}",
                    count,
                    ip.clone() + "}",
                    code,
                    if is_head { 0 } else { length.unwrap_or(0) },
                    request
                );
            }
            Ok(res.map(|body| LoggerFinalizer {
                body,
                count,
                ip,
                code,
                size: 0,
                start,
                body_start: Instant::now(),
                version,
                metrics,
                native,
            }))
        })
    }
}
pin_project! {
    pub(super) struct LoggerFinalizer {
        #[pin] body:Body,count:u64,ip:Option<String>,code:u16,size:u64,start:Instant,body_start:Instant,version:Version,metrics:Arc<Metrics>,
        native:HttpObservation,
    }
    impl PinnedDrop for LoggerFinalizer {
        fn drop(this:Pin<&mut Self>){
            if let Some(ip)=&this.ip {info!("{{{}/{:16} Code={} Byte={:<8} Released request after {}ms ({:.2} KB/s)",this.count,ip.clone()+"}",this.code,this.size,this.start.elapsed().as_millis(),this.size as f64/max(this.body_start.elapsed().as_millis(),1) as f64);}
            this.metrics.cache_sent_size.get_or_create(&vec![("version".to_owned(),format!("{:?}",this.version))]).inc_by(this.size);
            this.metrics.cache_sent_duration.get_or_create(&vec![("version".to_owned(),format!("{:?}",this.version))]).observe(this.start.elapsed().as_secs_f64());
        }
    }
}
impl HttpBody for LoggerFinalizer {
    type Data = <Body as HttpBody>::Data;
    type Error = <Body as HttpBody>::Error;
    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();
        let frame = this.body.as_mut().poll_frame(cx);
        if let Poll::Ready(value) = &frame {
            match value {
                Some(Ok(frame)) => {
                    if let Some(bytes) = frame.data_ref() {
                        *this.size = this.size.saturating_add(bytes.len() as u64);
                        #[cfg(all(feature = "manager-integration", target_os = "linux"))]
                        this.native.data(bytes.len() as u64);
                    }
                    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
                    if frame.is_trailers() || this.body.is_end_stream() {
                        this.native.end();
                    }
                }
                Some(Err(_)) => {
                    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
                    this.native.failed();
                }
                None => {
                    #[cfg(all(feature = "manager-integration", target_os = "linux"))]
                    this.native.end();
                }
            }
        }
        frame
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

#[cfg(all(test, feature = "manager-integration", target_os = "linux"))]
mod tests {
    use super::*;
    use axum::{Router, routing::get};
    use bytes::Bytes;
    use http_body_util::BodyExt;
    #[tokio::test]
    async fn actual_middleware_counts_body_error_drop_and_unpolled_future_only_once() {
        let observer = crate::manager_integration::ObservationHandle::new();
        let mut metrics = Metrics::new();
        metrics.observation = observer.clone();
        let router = Router::new().route(
            "/h/test",
            get(|| async {
                Response::builder()
                    .header(CONTENT_LENGTH, 9)
                    .body(Body::from_stream(futures::stream::iter([
                        Ok(Bytes::from_static(b"abc")),
                        Err(std::io::Error::other("private_error")),
                    ])))
                    .unwrap()
            }),
        );
        let mut service = Logger::new(Arc::new(metrics)).layer(router);
        let mut failed = service
            .call(Request::builder().uri("/h/test").body(Body::empty()).unwrap())
            .await
            .unwrap()
            .into_body();
        assert!(failed.frame().await.unwrap().is_ok());
        assert!(failed.frame().await.unwrap().is_err());
        drop(failed);
        let mut cancelled = service
            .call(Request::builder().uri("/h/test").body(Body::empty()).unwrap())
            .await
            .unwrap()
            .into_body();
        assert!(cancelled.frame().await.unwrap().is_ok());
        drop(cancelled);
        drop(service.call(Request::builder().uri("/h/test").body(Body::empty()).unwrap()));
        let payload = observer.http_snapshot().unwrap();
        let rows = payload["nativeHttp"].as_array().unwrap();
        assert!(rows.iter().any(|row| row[2] == 2 && row[3] == 2 && row[4] == 1));
        assert!(rows.iter().any(|row| row[2] == 2 && row[3] == 3 && row[4] == 1));
        assert!(rows.iter().any(|row| row[2] == 0 && row[3] == 3 && row[4] == 1));
        assert!(!rows.iter().any(|row| row[3] == 1));
        assert!(!payload.to_string().contains("private_error"));
    }
}

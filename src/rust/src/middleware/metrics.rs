use http::{Request, Response};
use http_body::Body;
use metrics::{counter, histogram};
use pin_project_lite::pin_project;
use std::{
    fmt,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    task::{ready, Context, Poll},
    time::Instant,
};
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use tower::{Layer, Service};

#[derive(Clone, Debug)]
pub struct ServerMetricsLayer;

impl<S> Layer<S> for ServerMetricsLayer {
    type Service = ServerMetrics<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ServerMetrics { inner }
    }
}

#[derive(Clone, Debug)]
pub struct ServerMetrics<S> {
    inner: S,
}

impl<S> ServerMetrics<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }

    pub fn layer() -> ServerMetricsLayer {
        ServerMetricsLayer
    }
}

impl<ReqBody, ResBody, S> Service<Request<ReqBody>> for ServerMetrics<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    ReqBody: Body,
    ResBody: Body,
    ResBody::Error: fmt::Display + 'static,
    S::Error: fmt::Display + 'static,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        // allow user defined function
        let start = Instant::now();
        let mut split = req.uri().path().split('/');
        let service = split.nth(1).unwrap_or("unknown").to_string();
        let method = split.next().unwrap_or("unknown").to_string();

        let remote_addr = req
            .extensions()
            .get::<TcpConnectInfo>()
            .and_then(|i| i.remote_addr())
            .or_else(|| {
                req.extensions()
                    .get::<TlsConnectInfo<TcpConnectInfo>>()
                    .and_then(|i| i.get_ref().remote_addr())
            });

        ResponseFuture {
            inner: self.inner.call(req),
            service,
            method,
            remote_addr,
            start,
        }
    }
}

pin_project! {
    pub struct ResponseFuture<F> {
        #[pin]
        pub inner: F,
        pub service: String,
        pub method: String,
        pub remote_addr: Option<SocketAddr>,
        pub start: Instant,
    }
}

impl<Fut, ResBody, E> Future for ResponseFuture<Fut>
where
    Fut: Future<Output = Result<Response<ResBody>, E>>,
    ResBody: Body,
    ResBody::Error: std::fmt::Display + 'static,
    E: std::fmt::Display + 'static,
{
    type Output = Result<Response<ResBody>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let result = ready!(this.inner.poll(cx));
        let latency = this.start.elapsed();
        let remote_addr = this
            .remote_addr
            .and_then(|addr| Some(addr.ip().to_string()))
            .unwrap_or(String::from("unknown"));

        match result {
            Ok(res) => {
                counter!("request",
                    "service" => this.service.clone(),
                    "method" => this.method.clone(),
                    "status" => res.status().as_u16().to_string(),
                    "remote_addr" => remote_addr
                )
                .increment(1);
                histogram!("requests_latency",
                    "service" => this.service.clone(),
                    "method" => this.method.clone())
                .record(latency.as_micros() as f64);
                Poll::Ready(Ok(res))
            }
            Err(err) => {
                // on_failure.on_failure(failure_class, latency, this.span);
                Poll::Ready(Err(err))
            }
        }
    }
}

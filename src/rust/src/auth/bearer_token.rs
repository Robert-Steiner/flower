use std::pin::Pin;

use futures::Future;
use http::Request;
use hyper::Body;

pub struct Config {
    pub t: u32,
}

pub fn noop_config<ResBody>(
    config: Config,
) -> impl Fn(Request<Body>) -> Pin<Box<dyn Future<Output = Result<Request<Body>, ResBody>> + Send>> + Clone
{
    move |request: http::Request<Body>| {
        Box::pin(async move {
            print!("{}", config.t);
            Ok(request)
        })
    }
}
// let a = <FleetServer<FleetService>>::NAME;
// let b = <HealthServer<tonic_health::server::HealthService>>::NAME;

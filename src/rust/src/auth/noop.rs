use http::Request;
use hyper::Body;

pub async fn noop<ResBody>(request: Request<Body>) -> Result<Request<Body>, ResBody> {
    Ok(request)
}

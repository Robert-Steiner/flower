use http::{Request, Response, StatusCode};
use tonic::body::{empty_body, BoxBody};

pub async fn unauthorized(
    _request: Request<hyper::Body>,
) -> Result<Request<hyper::Body>, Response<BoxBody>> {
    let unauthorized_response = Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .body(empty_body())
        .unwrap();

    Err(unauthorized_response)
}

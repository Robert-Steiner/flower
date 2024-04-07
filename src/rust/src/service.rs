mod common;
pub mod driver;
pub mod fleet;
mod convertion;
pub mod pb {
    tonic::include_proto!("flwr.proto");
}

mod common;
pub mod driver;
pub mod fleet;
pub mod pb {
    tonic::include_proto!("flwr.proto");
}

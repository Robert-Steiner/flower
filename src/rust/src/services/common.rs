use std::fmt::Display;

use field::field;
use tonic::{Code, Status};
use tonic_types::{ErrorDetails, FieldViolation, StatusExt};
use tracing::{error, info};

use crate::model::Node;

use super::pb;

pub const ANCESTRY_SEPARATOR: &str = ", ";

pub fn validation_err_into_grpc_err(mut err_details: ErrorDetails) -> Status {
    err_details.add_help_link("documentation", "https://flower.ai/docs/");
    info!("gRPC validation error: {:?}", err_details);
    Status::with_error_details(
        Code::InvalidArgument,
        "Request contains invalid arguments.",
        err_details,
    )
}

pub fn into_internal_server_err<E: Display>(err: E) -> Status {
    error!("internal server error: {err}");
    Status::internal("internal server error")
}

pub fn validate_node(node: &pb::Node, field_path: &str) -> Result<(), FieldViolation> {
    let field = format!("{}.{}", field_path, field!(node_id @ pb::Node));
    match (node.anonymous, node.node_id) {
        (false, 0) => {
            let violation = FieldViolation {
                field,
                description: "must be set for non-anonymous node".to_string(),
            };
            Err(violation)
        }
        (true, id) if id != 0 => {
            let violation = FieldViolation {
                field,
                description: "must be set for non-anonymous node".to_string(),
            };
            Err(violation)
        }
        _ => Ok(()),
    }
}

impl From<Node> for pb::Node {
    fn from(value: Node) -> Self {
        match value {
            Node::Id(id) => pb::Node {
                node_id: id,
                ..Default::default()
            },
            Node::Anonymous => pb::Node {
                anonymous: true,
                ..Default::default()
            },
        }
    }
}

impl From<pb::Node> for Node {
    fn from(value: pb::Node) -> Self {
        match value.anonymous {
            true => Self::Anonymous,
            false => Self::Id(value.node_id),
        }
    }
}

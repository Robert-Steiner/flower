use chrono::Utc;
use field::field;
use itertools::Itertools;
use prost::Message;
use tonic_types::{ErrorDetails, FieldViolation};

use crate::model::{
    self,
    handler::{PushTaskResultRequest, Task},
};

use super::{
    common::{validate_node, ValidationConfig, ANCESTRY_SEPARATOR},
    fleet::Config,
    pb,
};

impl TryFrom<(pb::PushTaskResRequest, &Config)> for PushTaskResultRequest {
    type Error = ErrorDetails;

    fn try_from(
        (mut request, config): (pb::PushTaskResRequest, &Config),
    ) -> Result<Self, Self::Error> {
        let mut err_details = ErrorDetails::new();

        let path = field!(task_res_list @ pb::PushTaskResRequest);
        let task_res = match request.task_res_list.len() {
            count if count != 1 => {
                err_details.add_bad_request_violation(
                    path,
                    format!("Must contain a single item but has {count}."),
                );
                return Err(err_details);
            }
            _ => request.task_res_list.pop().unwrap(),
        };

        if !task_res.task_id.is_empty() {
            err_details.add_bad_request_violation(
                format!("{}.{}", path, field!(task_id @ pb::TaskRes)),
                "Must be empty.",
            );
            return Err(err_details);
        }

        let path = format!("{}.{}", path, field!(task @ pb::TaskRes));
        let task = match task_res.task {
            Some(task) => task,
            None => {
                err_details.add_bad_request_violation(path, "Must not be empty.");
                return Err(err_details);
            }
        };

        if task.ancestry.is_empty() {
            err_details.add_bad_request_violation(
                format!("{}.{}", path, field!(ancestry @ pb::Task)),
                "Must not be empty.",
            );
            return Err(err_details);
        }

        let task_ancestry = task.ancestry.clone().into_iter().join(ANCESTRY_SEPARATOR);

        let task: Task = (task, config as &dyn ValidationConfig, path.as_str())
            .try_into()
            .map_err(|violation: FieldViolation| {
                err_details.add_bad_request_violation(violation.field, violation.description);
                err_details
            })?;

        Ok(Self {
            group_id: task_res.group_id,
            run_id: task_res.run_id,
            task,
            task_ancestry,
        })
    }
}

impl TryFrom<(pb::Task, &dyn ValidationConfig, &str)> for Task {
    type Error = FieldViolation;

    fn try_from(
        (task, config, path): (pb::Task, &dyn ValidationConfig, &str),
    ) -> Result<Self, Self::Error> {
        let now = Utc::now().timestamp() as f64;

        let created_at = task.created_at.floor(); // round time down as `now` does not have decimal places
        if created_at > now {
            return Err(FieldViolation {
                field: format!("{path}.{}", field!(created_at @ pb::Task)),
                description: format!(
                    "Message was created in the future. now: {now}, created_at: {}",
                    task.created_at
                ),
            });
        }

        if now - created_at > config.message_expires_after().as_secs_f64() {
            return Err(FieldViolation {
                field: format!("{path}.{}", field!(created_at @ pb::Task)),
                description: format!(
                    "Message is too old. Time difference: {} seconds > {} seconds",
                    now - created_at,
                    config.message_expires_after().as_secs_f64()
                ),
            });
        }

        if !task.delivered_at.is_empty() {
            return Err(FieldViolation {
                field: format!("{}.{}", path, field!(delivered_at @ pb::Task)),
                description: "Must be empty.".to_string(),
            });
        }

        if task.pushed_at != 0. {
            return Err(FieldViolation {
                field: format!("{}.{}", path, field!(pushed_at @ pb::Task)),
                description: "Should not be set by client.".to_string(),
            });
        }

        if task.ttl <= 0. {
            return Err(FieldViolation {
                field: format!("{}.{}", path, field!(ttl @ pb::Task)),
                description: "Must be higher than zero.".to_string(),
            });
        }

        let producer = match task.producer {
            Some(producer) => producer,
            None => {
                return Err(FieldViolation {
                    field: format!("{}.{}", path, field!(producer @ pb::Task)),
                    description: "Must not be empty.".to_string(),
                });
            }
        };

        let producer_path = format!("{}.{}", path, field!(producer @ pb::Task));
        validate_node(&producer, &producer_path)?;

        let consumer = match task.consumer {
            Some(consumer) => consumer,
            None => {
                return Err(FieldViolation {
                    field: format!("{}.{}", path, field!(consumer @ pb::Task)),
                    description: "Must not be empty.".to_string(),
                })
            }
        };

        let consumer_path = format!("{}.{}", path, field!(consumer @ pb::Task));
        validate_node(&consumer, &consumer_path)?;

        if task.task_type.is_empty() {
            return Err(FieldViolation {
                field: format!("{}.{}", path, field!(task_type @ pb::Task)),
                description: "Must not be empty.".to_string(),
            });
        }

        let result = (task.recordset, task.error, path).try_into()?;

        Ok(Self {
            producer: producer.into(),
            consumer: consumer.into(),
            created_at: task.created_at,
            delivered_at: task.delivered_at,
            ttl: task.ttl,
            task_type: task.task_type,
            result,
        })
    }
}

impl TryFrom<(Option<pb::RecordSet>, Option<pb::Error>, &str)> for model::handler::Result {
    type Error = FieldViolation;

    fn try_from(
        (recordset, error, path): (Option<pb::RecordSet>, Option<pb::Error>, &str),
    ) -> Result<Self, <model::handler::Result as TryFrom<(Option<pb::RecordSet>, Option<pb::Error>, &str)>>::Error>{
        let field = format!(
            "{path}.{} | {path}.{}",
            field!(recordset @ pb::Task),
            field!(error @ pb::Task)
        );

        match (recordset, error) {
            (Some(recordset), None) => Ok(model::handler::Result::RecordSet(
                Message::encode_to_vec(&recordset),
            )),
            (None, Some(error)) => Ok(model::handler::Result::Error(error.into())),
            (Some(_), Some(_)) => Err(FieldViolation {
                field,
                description: "Only One of the fields must not be set.".to_string(),
            }),
            (None, None) => Err(FieldViolation {
                field,
                description: "One of the fields must not be empty.".to_string(),
            }),
        }
    }
}

impl From<pb::Error> for model::handler::Error {
    fn from(value: pb::Error) -> Self {
        Self {
            code: value.code,
            reason: value.reason,
        }
    }
}

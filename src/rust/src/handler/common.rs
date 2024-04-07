use chrono::Utc;
use rand::{
    distributions::{Distribution, Uniform},
    thread_rng, Rng,
};
use uuid::Uuid;

use crate::model::{
    handler::{self, NewTaskInstructionOrResult},
    state::TaskInstructionOrResult,
};

pub fn new_id() -> i64 {
    struct Filter<Dist, Test> {
        dist: Dist,
        test: Test,
    }

    impl<T, Dist, Test> Distribution<T> for Filter<Dist, Test>
    where
        Dist: Distribution<T>,
        Test: Fn(&T) -> bool,
    {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> T {
            loop {
                let x = self.dist.sample(rng);
                if (self.test)(&x) {
                    return x;
                }
            }
        }
    }

    let mut rng = thread_rng();
    let dist = Filter {
        dist: Uniform::new(i64::MIN, i64::MAX),
        test: |x: &_| (x != &0),
    };

    rng.sample(&dist)
}

pub(super) fn new_task_instruction_or_result(
    request: NewTaskInstructionOrResult,
) -> (Uuid, TaskInstructionOrResult) {
    if let handler::Result::RecordSet(recordset) = request.task.result {
        let id = Uuid::new_v4();
        let pushed_at = Utc::now().timestamp() as f64;
        let (producer_node_id, producer_anonymous) = (&request.task.producer).into();
        let (consumer_node_id, consumer_anonymous) = (&request.task.consumer).into();
        (
            id,
            TaskInstructionOrResult {
                id,
                group_id: request.group_id,
                run_id: request.run_id,
                producer_node_id,
                producer_anonymous,
                consumer_node_id,
                consumer_anonymous,
                created_at: request.task.created_at,
                delivered_at: request.task.delivered_at,
                pushed_at,
                ttl: request.task.ttl,
                ancestry: request.task_ancestry,
                task_type: request.task.task_type,
                recordset,
            },
        )
    } else {
        unimplemented!()
    }
}

use std::{sync::LazyLock, time::Duration};

use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Histogram},
};

use crate::enums::work_status::WorkStatus;

struct Metrics {
    worker_triggers: Counter<u64>,
    worker_trigger_duration: Histogram<f64>,
}

static METRICS: LazyLock<Metrics> = LazyLock::new(|| {
    let meter = global::meter(env!("CARGO_PKG_NAME"));
    Metrics {
        worker_triggers: meter
            .u64_counter("scheduler.worker.triggers")
            .with_description("Scrape requests sent to workers")
            .build(),
        worker_trigger_duration: meter
            .f64_histogram("scheduler.worker.trigger.duration")
            .with_unit("s")
            .with_description("Time until a worker answered a scrape request")
            .with_boundaries(vec![0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 90.0, 120.0])
            .build(),
    }
});

pub fn record_trigger(worker: &str, status: &WorkStatus, duration: Duration) {
    let outcome = match status {
        WorkStatus::Completed => "completed",
        WorkStatus::Failed => "failed",
    };
    let attributes = [
        KeyValue::new("worker", worker.to_owned()),
        KeyValue::new("outcome", outcome),
    ];
    METRICS.worker_triggers.add(1, &attributes);
    METRICS
        .worker_trigger_duration
        .record(duration.as_secs_f64(), &attributes);
}

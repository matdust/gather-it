use std::{sync::LazyLock, time::Duration};

use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Histogram},
};

struct Metrics {
    messages_received: Counter<u64>,
    job_ads_received: Counter<u64>,
    job_ads_inserted: Counter<u64>,
    job_ads_skipped: Counter<u64>,
    flush_duration: Histogram<f64>,
    flush_retries: Counter<u64>,
}

static METRICS: LazyLock<Metrics> = LazyLock::new(|| {
    let meter = global::meter(env!("CARGO_PKG_NAME"));
    Metrics {
        messages_received: meter
            .u64_counter("binder.messages.received")
            .with_description("RabbitMQ messages consumed, rejected ones failed to deserialize")
            .build(),
        job_ads_received: meter
            .u64_counter("binder.job_ads.received")
            .with_description("Job ads received in accepted messages")
            .build(),
        job_ads_inserted: meter
            .u64_counter("binder.job_ads.inserted")
            .with_description("Job ads written to the database")
            .build(),
        job_ads_skipped: meter
            .u64_counter("binder.job_ads.skipped")
            .with_description("Job ads already in the database or duplicated within a flush")
            .build(),
        flush_duration: meter
            .f64_histogram("binder.flush.duration")
            .with_unit("s")
            .with_description("Duration of a flush, including retries")
            .with_boundaries(vec![0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0])
            .build(),
        flush_retries: meter
            .u64_counter("binder.flush.retries")
            .with_description("Failed flush attempts that were retried")
            .build(),
    }
});

pub fn record_message(accepted: bool, job_ads: usize) {
    let outcome = if accepted { "accepted" } else { "rejected" };
    METRICS
        .messages_received
        .add(1, &[KeyValue::new("outcome", outcome)]);
    METRICS.job_ads_received.add(job_ads as u64, &[]);
}

pub fn record_flush(trigger: &'static str, duration: Duration, inserted: usize, skipped: usize) {
    METRICS
        .flush_duration
        .record(duration.as_secs_f64(), &[KeyValue::new("trigger", trigger)]);
    METRICS.job_ads_inserted.add(inserted as u64, &[]);
    METRICS.job_ads_skipped.add(skipped as u64, &[]);
}

pub fn record_flush_retry() {
    METRICS.flush_retries.add(1, &[]);
}

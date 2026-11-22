use std::{sync::LazyLock, time::Duration};

use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Histogram},
};

struct Metrics {
    pages_fetched: Counter<u64>,
    offers_scraped: Counter<u64>,
    offers_published: Counter<u64>,
    run_duration: Histogram<f64>,
}

static METRICS: LazyLock<Metrics> = LazyLock::new(|| {
    let meter = global::meter(env!("CARGO_PKG_NAME"));
    Metrics {
        pages_fetched: meter
            .u64_counter("scraper.pages.fetched")
            .with_description("Listing pages fetched from the job board")
            .build(),
        offers_scraped: meter
            .u64_counter("scraper.offers.scraped")
            .with_description("Job offers parsed from fetched pages")
            .build(),
        offers_published: meter
            .u64_counter("scraper.offers.published")
            .with_description("Job offers published to RabbitMQ")
            .build(),
        run_duration: meter
            .f64_histogram("scraper.run.duration")
            .with_unit("s")
            .with_description("Duration of a whole scrape run")
            .with_boundaries(vec![1.0, 5.0, 10.0, 30.0, 60.0, 90.0, 120.0, 300.0])
            .build(),
    }
});

fn outcome(success: bool) -> KeyValue {
    KeyValue::new("outcome", if success { "ok" } else { "error" })
}

pub fn record_page_fetch(success: bool) {
    METRICS.pages_fetched.add(1, &[outcome(success)]);
}

pub fn record_offers_scraped(count: usize) {
    METRICS.offers_scraped.add(count as u64, &[]);
}

pub fn record_offers_published(count: usize) {
    METRICS.offers_published.add(count as u64, &[]);
}

pub fn record_run(success: bool, duration: Duration) {
    METRICS
        .run_duration
        .record(duration.as_secs_f64(), &[outcome(success)]);
}

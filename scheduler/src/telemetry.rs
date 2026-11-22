use std::{sync::LazyLock, time::Instant};

use axum::{
    extract::{MatchedPath, Request},
    http::HeaderMap,
    middleware::Next,
    response::Response,
};
use opentelemetry::{KeyValue, global, metrics::Histogram, trace::TracerProvider as _};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_http::{HeaderExtractor, HeaderInjector};
use opentelemetry_otlp::{ExporterBuildError, LogExporter, MetricExporter, SpanExporter};
use opentelemetry_sdk::{
    Resource, logs::SdkLoggerProvider, metrics::SdkMeterProvider,
    propagation::TraceContextPropagator, trace::SdkTracerProvider,
};
use tracing::{Instrument, field::Empty};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{
    EnvFilter, Layer, filter::filter_fn, layer::SubscriberExt, util::SubscriberInitExt,
};

// crates used by the OTLP exporter itself, exporting their events would feed the exporter its own output
const EXPORTER_TARGETS: [&str; 4] = ["opentelemetry", "hyper", "reqwest", "h2"];

static HTTP_REQUEST_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    global::meter(env!("CARGO_PKG_NAME"))
        .f64_histogram("http.server.request.duration")
        .with_unit("s")
        .with_description("Duration of HTTP server requests")
        .with_boundaries(vec![
            0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
        ])
        .build()
});

pub struct Telemetry {
    traces: SdkTracerProvider,
    metrics: SdkMeterProvider,
    logs: SdkLoggerProvider,
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        if let Err(e) = self.traces.shutdown() {
            eprintln!("Failed to shut down tracer provider: {e}");
        }
        if let Err(e) = self.metrics.shutdown() {
            eprintln!("Failed to shut down meter provider: {e}");
        }
        if let Err(e) = self.logs.shutdown() {
            eprintln!("Failed to shut down logger provider: {e}");
        }
    }
}

pub fn init() -> Result<Option<Telemetry>, ExporterBuildError> {
    let registry = tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer());

    if std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_none() {
        registry.init();
        return Ok(None);
    }

    let resource = Resource::builder().build();
    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(SpanExporter::builder().with_http().build()?)
        .build();
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_periodic_exporter(MetricExporter::builder().with_http().build()?)
        .build();
    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(LogExporter::builder().with_http().build()?)
        .build();

    global::set_text_map_propagator(TraceContextPropagator::new());
    global::set_tracer_provider(tracer_provider.clone());
    global::set_meter_provider(meter_provider.clone());

    let not_exporter = filter_fn(|metadata| {
        !EXPORTER_TARGETS
            .iter()
            .any(|target| metadata.target().starts_with(target))
    });
    registry
        .with(
            tracing_opentelemetry::layer()
                .with_tracer(tracer_provider.tracer(env!("CARGO_PKG_NAME")))
                .with_filter(not_exporter.clone()),
        )
        .with(OpenTelemetryTracingBridge::new(&logger_provider).with_filter(not_exporter))
        .init();

    Ok(Some(Telemetry {
        traces: tracer_provider,
        metrics: meter_provider,
        logs: logger_provider,
    }))
}

pub async fn trace_http(request: Request, next: Next) -> Response {
    let start = Instant::now();
    let method = request.method().to_string();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| request.uri().path(), MatchedPath::as_str)
        .to_owned();
    let span = tracing::info_span!(
        "http request",
        otel.name = format!("{method} {route}"),
        otel.kind = "server",
        otel.status_code = Empty,
        http.request.method = method,
        http.route = route,
        http.response.status_code = Empty,
    );
    let parent = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(request.headers()))
    });
    let _ = span.set_parent(parent);

    let response = next.run(request).instrument(span.clone()).await;

    let status = response.status();
    span.record("http.response.status_code", status.as_u16());
    if status.is_server_error() {
        span.record("otel.status_code", "error");
    }
    HTTP_REQUEST_DURATION.record(
        start.elapsed().as_secs_f64(),
        &[
            KeyValue::new("http.request.method", method),
            KeyValue::new("http.route", route),
            KeyValue::new("http.response.status_code", i64::from(status.as_u16())),
        ],
    );
    response
}

pub fn trace_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(
            &tracing::Span::current().context(),
            &mut HeaderInjector(&mut headers),
        );
    });
    headers
}

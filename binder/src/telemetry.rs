use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{ExporterBuildError, LogExporter, MetricExporter, SpanExporter};
use opentelemetry_sdk::{
    Resource, logs::SdkLoggerProvider, metrics::SdkMeterProvider,
    propagation::TraceContextPropagator, trace::SdkTracerProvider,
};
use tracing_subscriber::{
    EnvFilter, Layer, filter::filter_fn, layer::SubscriberExt, util::SubscriberInitExt,
};

// crates used by the OTLP exporter itself, exporting their events would feed the exporter its own output
const EXPORTER_TARGETS: [&str; 4] = ["opentelemetry", "hyper", "reqwest", "h2"];

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

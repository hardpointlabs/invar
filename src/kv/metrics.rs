use std::sync::Arc;

use slatedb_common::metrics::{CounterFn, GaugeFn, HistogramFn, MetricsRecorder, UpDownCounterFn};

struct MetricsRsCounter(metrics::Counter);

impl CounterFn for MetricsRsCounter {
    fn increment(&self, value: u64) {
        self.0.increment(value);
    }
}

struct MetricsRsGauge(metrics::Gauge);

impl GaugeFn for MetricsRsGauge {
    fn set(&self, value: i64) {
        self.0.set(value as f64);
    }
}

struct MetricsRsUpDownCounter(metrics::Gauge);

impl UpDownCounterFn for MetricsRsUpDownCounter {
    fn increment(&self, value: i64) {
        if value >= 0 {
            self.0.increment(value as f64);
        } else {
            self.0.decrement((-value) as f64);
        }
    }
}

struct MetricsRsHistogram(metrics::Histogram);

impl HistogramFn for MetricsRsHistogram {
    fn record(&self, value: f64) {
        self.0.record(value);
    }
}

pub struct MetricsRsRecorder;

impl MetricsRecorder for MetricsRsRecorder {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        let labels: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        metrics::describe_counter!(name.to_string(), description.to_string());
        Arc::new(MetricsRsCounter(metrics::counter!(
            name.to_string(),
            &labels
        )))
    }

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        let labels: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        metrics::describe_gauge!(name.to_string(), description.to_string());
        Arc::new(MetricsRsGauge(metrics::gauge!(
            name.to_string(),
            &labels
        )))
    }

    fn register_up_down_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn UpDownCounterFn> {
        let labels: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        metrics::describe_counter!(name.to_string(), description.to_string());
        Arc::new(MetricsRsUpDownCounter(metrics::gauge!(
            name.to_string(),
            &labels
        )))
    }

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
        _boundaries: &[f64],
    ) -> Arc<dyn HistogramFn> {
        let labels: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        metrics::describe_histogram!(name.to_string(), description.to_string());
        Arc::new(MetricsRsHistogram(metrics::histogram!(
            name.to_string(),
            &labels
        )))
    }
}

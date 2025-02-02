use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScraperMetrics {
    pub requests_per_second: f64,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub avg_response_time_ms: f64,
    pub rate_limiter_wait_time_ms: f64,
    pub last_error: Option<String>,
    pub last_error_time: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct MetricsCollector {
    metrics: Arc<Mutex<ScraperMetrics>>,
    last_update: Arc<Mutex<Instant>>,
}

impl Default for ScraperMetrics {
    fn default() -> Self {
        Self {
            requests_per_second: 0.0,
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            avg_response_time_ms: 0.0,
            rate_limiter_wait_time_ms: 0.0,
            last_error: None,
            last_error_time: None,
        }
    }
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(Mutex::new(ScraperMetrics::default())),
            last_update: Arc::new(Mutex::new(Instant::now())),
        }
    }

    pub fn record_request_start(&self) -> RequestTracker {
        RequestTracker {
            start_time: Instant::now(),
            collector: self.clone(),
        }
    }

    pub fn record_rate_limit_wait(&self, duration: Duration) {
        let mut metrics = self.metrics.lock().unwrap();
        metrics.rate_limiter_wait_time_ms = duration.as_millis() as f64;
    }

    pub fn record_error(&self, error: String) {
        let mut metrics = self.metrics.lock().unwrap();
        metrics.last_error = Some(error);
        metrics.last_error_time = Some(Utc::now());
        metrics.failed_requests += 1;
    }

    pub fn get_metrics(&self) -> ScraperMetrics {
        self.metrics.lock().unwrap().clone()
    }
}

pub struct RequestTracker {
    start_time: Instant,
    collector: MetricsCollector,
}

impl RequestTracker {
    pub fn finish(self, success: bool) {
        let duration = self.start_time.elapsed();
        let mut metrics = self.collector.metrics.lock().unwrap();
        
        metrics.total_requests += 1;
        if success {
            metrics.successful_requests += 1;
        } else {
            metrics.failed_requests += 1;
        }

        // Update average response time using exponential moving average
        let alpha = 0.1; // Smoothing factor
        let new_avg = metrics.avg_response_time_ms * (1.0 - alpha) + 
                     duration.as_millis() as f64 * alpha;
        metrics.avg_response_time_ms = new_avg;

        // Update requests per second
        let mut last_update = self.collector.last_update.lock().unwrap();
        let elapsed = last_update.elapsed();
        if elapsed >= Duration::from_secs(1) {
            metrics.requests_per_second = metrics.total_requests as f64 / elapsed.as_secs_f64();
            *last_update = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[test]
    fn test_metrics_collector_basic() {
        let collector = MetricsCollector::new();
        let tracker = collector.record_request_start();
        
        // Simulate some work
        std::thread::sleep(Duration::from_millis(10));
        
        tracker.finish(true);
        let metrics = collector.get_metrics();
        
        assert_eq!(metrics.total_requests, 1);
        assert_eq!(metrics.successful_requests, 1);
        assert_eq!(metrics.failed_requests, 0);
        assert!(metrics.avg_response_time_ms > 0.0);
    }

    #[test]
    fn test_metrics_collector_failed_request() {
        let collector = MetricsCollector::new();
        let tracker = collector.record_request_start();
        
        tracker.finish(false);
        let metrics = collector.get_metrics();
        
        assert_eq!(metrics.total_requests, 1);
        assert_eq!(metrics.successful_requests, 0);
        assert_eq!(metrics.failed_requests, 1);
    }

    #[test]
    fn test_metrics_collector_rate_limit() {
        let collector = MetricsCollector::new();
        collector.record_rate_limit_wait(Duration::from_millis(100));
        
        let metrics = collector.get_metrics();
        assert_eq!(metrics.rate_limiter_wait_time_ms, 100.0);
    }

    #[test]
    fn test_metrics_collector_error() {
        let collector = MetricsCollector::new();
        collector.record_error("Test error".to_string());
        
        let metrics = collector.get_metrics();
        assert_eq!(metrics.failed_requests, 1);
        assert_eq!(metrics.last_error, Some("Test error".to_string()));
        assert!(metrics.last_error_time.is_some());
    }

    #[test]
    fn test_metrics_collector_multiple_requests() {
        let collector = MetricsCollector::new();
        
        // Simulate multiple requests
        for i in 0..5 {
            let tracker = collector.record_request_start();
            std::thread::sleep(Duration::from_millis(10));
            tracker.finish(i % 2 == 0); // Alternate between success and failure
        }
        
        let metrics = collector.get_metrics();
        assert_eq!(metrics.total_requests, 5);
        assert_eq!(metrics.successful_requests, 3); // 0, 2, 4 are successful
        assert_eq!(metrics.failed_requests, 2); // 1, 3 are failed
        assert!(metrics.avg_response_time_ms > 0.0);
    }

    #[tokio::test]
    async fn test_metrics_collector_rps() {
        let collector = MetricsCollector::new();
        
        // Simulate requests over time
        for _ in 0..10 {
            let tracker = collector.record_request_start();
            sleep(Duration::from_millis(100)).await;
            tracker.finish(true);
        }
        
        let metrics = collector.get_metrics();
        assert!(metrics.requests_per_second > 0.0);
        assert!(metrics.requests_per_second <= 10.0); // Should be around 10 rps
    }
} 
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use questdb::ingress::{Buffer, ColumnName, Sender, TableName, TimestampNanos};
use serde::Serialize;
use std::{
    collections::VecDeque,
    num::NonZeroU32,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use tracing::{error, info};
use urlencoding;

use crate::{
    config::ScraperConfig,
    metrics::ScraperMetrics,
};

const CONCURRENT_REQUESTS: u32 = 4;
const BASE_URL: &str = "https://www.mkttl.co.uk/matches/team";
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Serialize)]
pub struct MatchHtmlStats {
    pub requests_per_second: f64,
    pub total_matches_processed: u64,
    pub total_matches_to_process: usize,
    pub latest_update: Option<DateTime<Utc>>,
    pub status: String,
    pub metrics: Option<ScraperMetrics>,
}

pub struct MatchHtmlScraper {
    client: reqwest::blocking::Client,
    quest_url: String,
    rate_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    stats: Arc<Mutex<MatchHtmlStats>>,
}

impl MatchHtmlScraper {
    pub fn new(config: ScraperConfig) -> Result<Self> {
        let client = reqwest::blocking::Client::builder()
            .user_agent(&config.scraping.user_agent)
            .timeout(Duration::from_secs(config.scraping.request_timeout_secs))
            .build()
            .context("Failed to create HTTP client")?;

        let stats = Arc::new(Mutex::new(MatchHtmlStats {
            requests_per_second: 0.0,
            total_matches_processed: 0,
            total_matches_to_process: 0,
            latest_update: None,
            status: "Initializing".to_string(),
            metrics: None,
        }));

        let quota = Quota::per_second(
            NonZeroU32::new(config.rate_limits.requests_per_second)
                .ok_or_else(|| anyhow!("Invalid requests_per_second value"))?,
        );
        let rate_limiter = Arc::new(RateLimiter::direct(quota));

        Ok(Self {
            client,
            quest_url: config.get_questdb_url(),
            rate_limiter,
            stats,
        })
    }

    fn validate_html(html: &str) -> bool {
        html.contains("div id=\"games\"")
    }

    fn retry_with_backoff<F, T>(mut operation: F) -> Result<T>
    where
        F: FnMut() -> Result<T>,
    {
        let mut delay = INITIAL_RETRY_DELAY;
        let mut attempt = 1;

        loop {
            match operation() {
                Ok(value) => return Ok(value),
                Err(e) => {
                    if attempt >= MAX_RETRIES {
                        return Err(e.context("Max retries exceeded"));
                    }
                    info!("Retry attempt {} after error: {}", attempt, e);
                    thread::sleep(delay);
                    delay *= 2;
                    attempt += 1;
                }
            }
        }
    }

    pub fn run(&mut self) -> Result<()> {
        info!("Starting HTML scraper");

        // Test QuestDB connection
        info!("Testing QuestDB connection...");
        {
            let mut buffer = Buffer::new();
            info!("Creating test buffer");
            buffer
                .table("mkttl_matches")?
                .column_str("url", "test")?
                .column_str("raw_html", "test")?
                .at(TimestampNanos::now())?;

            info!("Attempting to flush test data to QuestDB");
            let mut sender = Sender::from_conf(&self.quest_url)?;
            match sender.flush(&mut buffer) {
                Ok(_) => info!("Successfully connected to QuestDB"),
                Err(e) => {
                    error!("Failed to connect to QuestDB: {}", e);
                    anyhow::bail!("QuestDB connection failed: {}. Please ensure QuestDB is running and accessible.", e);
                }
            }
        }

        // Query QuestDB for matches without HTML content
        let urls = {
            let mut stats = self.stats.lock().unwrap();
            stats.status = "Querying QuestDB for matches without HTML...".to_string();

            // Use reqwest to query QuestDB's REST API
            let query = "SELECT url, updated_by FROM mkttl_matches WHERE raw_html IS NULL";
            let quest_query_url = format!(
                "http://localhost:9000/exec?query={}",
                urlencoding::encode(query)
            );

            info!("Querying QuestDB for matches without HTML");
            let response = self
                .client
                .get(&quest_query_url)
                .send()
                .context("Failed to query QuestDB")?;

            if !response.status().is_success() {
                anyhow::bail!("Failed to query QuestDB: HTTP {}", response.status());
            }

            let json: serde_json::Value = response
                .json()
                .context("Failed to parse QuestDB response")?;

            let urls_and_updaters: Vec<(String, Option<String>)> = json["dataset"]
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .filter_map(|row| {
                    let url = row[0].as_str()?;
                    let updated_by = row[1].as_str().map(String::from);
                    Some((url.to_string(), updated_by))
                })
                .collect();

            info!("Found {} matches without HTML", urls_and_updaters.len());
            stats.total_matches_to_process = urls_and_updaters.len();
            stats.status = format!("Processing {} URLs", urls_and_updaters.len());
            urls_and_updaters
        };

        let queue = Arc::new(Mutex::new(VecDeque::from(urls.clone())));
        info!("Starting {} concurrent workers", CONCURRENT_REQUESTS);

        let mut handles = Vec::new();
        for i in 0..CONCURRENT_REQUESTS {
            let queue = Arc::clone(&queue);
            let rate_limiter = Arc::clone(&self.rate_limiter);
            let client = self.client.clone();
            let quest_url = self.quest_url.clone();
            let stats = Arc::clone(&self.stats);

            let handle = thread::spawn(move || -> Result<()> {
                info!("Worker {} started", i);
                while let Some((url, updated_by)) = {
                    let mut queue = queue.lock().unwrap();
                    info!("Worker {} checking queue (size: {})", i, queue.len());
                    queue.pop_front()
                } {
                    info!(
                        "Worker {} processing URL: {}, updated_by: {:?}",
                        i, url, updated_by
                    );
                    {
                        let mut stats = stats.lock().unwrap();
                        stats.status = format!("Worker {} processing URL: {}", i, url);
                    }

                    // Wait for rate limiter
                    info!("Worker {} waiting for rate limiter", i);
                    while let Err(_) = rate_limiter.check() {
                        thread::sleep(Duration::from_millis(100));
                    }
                    info!("Worker {} passed rate limiter", i);

                    let full_url = format!("{}/{}", BASE_URL, url);
                    let result = Self::retry_with_backoff(|| {
                        info!("Worker {} fetching HTML from {}", i, full_url);
                        let response = client.get(&full_url).send()?;

                        if !response.status().is_success() {
                            anyhow::bail!("Failed to fetch HTML: HTTP {}", response.status());
                        }

                        let html = response.text()?;
                        info!(
                            "Worker {} successfully downloaded HTML ({} bytes) for {}",
                            i,
                            html.len(),
                            url
                        );

                        // Validate HTML
                        if !Self::validate_html(&html) {
                            anyhow::bail!("Invalid HTML - missing games div");
                        }
                        info!("Worker {} successfully validated HTML for {}", i, url);

                        // Store the HTML
                        let mut buffer = Buffer::new();
                        let now = TimestampNanos::now();
                        let table = TableName::new("mkttl_matches")?;
                        let updated_by_name = ColumnName::new("updated_by")?;
                        let url_name = ColumnName::new("url")?;
                        let raw_html_name = ColumnName::new("raw_html")?;
                        let fetched_at_name = ColumnName::new("fetched_at")?;
                        // Preserve the updated_by if it exists
                        if let Some(ref updated_by_val) = updated_by {
                            buffer
                                .table(table)?
                                .symbol(updated_by_name, updated_by_val)?
                                .column_str(url_name, &url)?
                                .column_str(raw_html_name, &html)?
                                .column_ts(fetched_at_name, now)?;
                        } else {
                            buffer
                                .table(table)?
                                .column_str(url_name, &url)?
                                .column_str(raw_html_name, &html)?
                                .column_ts(fetched_at_name, now)?;
                        }

                        let mut sender = Sender::from_conf(&quest_url)?;
                        sender.flush(&mut buffer)?;
                        info!("Worker {} successfully stored HTML for {}", i, url);

                        Ok(())
                    });

                    match result {
                        Ok(_) => {
                            info!("Worker {} successfully processed {}", i, url);
                            let mut stats = stats.lock().unwrap();
                            stats.total_matches_processed += 1;
                            stats.latest_update = Some(Utc::now());
                            stats.status = format!("Successfully processed URL: {}", url);
                            let elapsed = stats
                                .latest_update
                                .unwrap()
                                .signed_duration_since(Utc::now())
                                .num_seconds()
                                .abs() as f64;
                            if elapsed > 0.0 {
                                stats.requests_per_second =
                                    stats.total_matches_processed as f64 / elapsed;
                            }
                            stats.total_matches_to_process = queue.lock().unwrap().len();
                        }
                        Err(e) => {
                            error!("Worker {} failed to process {}: {}", i, url, e);
                            let mut stats = stats.lock().unwrap();
                            stats.status = format!("Error processing URL {}: {}", url, e);
                            continue;
                        }
                    }
                }
                info!("Worker {} finished", i);
                Ok(())
            });
            handles.push(handle);
        }

        info!("Waiting for workers to complete");
        for handle in handles {
            match handle.join() {
                Ok(result) => {
                    if let Err(e) = result {
                        error!("Worker failed with error: {}", e);
                    }
                }
                Err(e) => {
                    error!("Worker panicked: {:?}", e);
                }
            }
        }
        info!("All workers completed");

        // Update final status
        {
            let mut stats = self.stats.lock().unwrap();
            stats.status = format!(
                "Completed processing {} URLs",
                stats.total_matches_processed
            );
        }

        Ok(())
    }
}

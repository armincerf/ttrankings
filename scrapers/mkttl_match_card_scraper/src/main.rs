mod config;
mod game_scraper;
mod types;
mod utils;
mod league_match;
mod cup_match;
mod match_html_scraper;
mod metrics;
mod web;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use questdb::ingress::{Buffer, Sender, TimestampNanos};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::num::NonZeroU32;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::signal;
use tracing::{error, info};
use urlencoding;

use config::ScraperConfig;
use match_html_scraper::MatchHtmlScraper;
use metrics::MetricsCollector;
use web::{AppState, ScraperStats};
use game_scraper::GameScraper;

const EVENT_LOG_BASE_URL: &str = "https://www.mkttl.co.uk/event-viewer/load.js";
const FULL_SCRAPE_PAGE_SIZE: u32 = 500;
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Deserialize, Clone)]
struct EventLogResponse {
    data: Vec<Vec<String>>,
    #[serde(rename = "recordsTotal")]
    records_total: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MatchEvent {
    timestamp: DateTime<Utc>,
    url: String,
    updated_by: String,
}

#[derive(Debug)]
struct DataTablesColumn {
    data: usize,
    name: String,
    searchable: bool,
    orderable: bool,
    search_value: String,
    search_regex: bool,
}

#[derive(Debug)]
struct DataTablesParams {
    draw: u32,
    start: u32,
    length: u32,
    columns: Vec<DataTablesColumn>,
    order_column: usize,
    order_dir: String,
    order_name: String,
    search_value: String,
    search_regex: bool,
    pages: u32,
    pagelength: u32,
}

impl DataTablesParams {
    fn new(page: u32, page_length: u32) -> Self {
        let columns = vec![
            DataTablesColumn {
                data: 0,
                name: "user.username".to_string(),
                searchable: true,
                orderable: true,
                search_value: String::new(),
                search_regex: false,
            },
            DataTablesColumn {
                data: 1,
                name: "me.log_updated".to_string(),
                searchable: true,
                orderable: true,
                search_value: String::new(),
                search_regex: false,
            },
            DataTablesColumn {
                data: 2,
                name: "system_event_log_type.object_description".to_string(),
                searchable: true,
                orderable: true,
                search_value: String::new(),
                search_regex: false,
            },
            DataTablesColumn {
                data: 3,
                name: "description".to_string(),
                searchable: true,
                orderable: false,
                search_value: String::new(),
                search_regex: false,
            },
        ];

        Self {
            draw: 1,
            start: page * page_length,
            length: page_length,
            columns,
            order_column: 1,
            order_dir: "desc".to_string(),
            order_name: "me.log_updated".to_string(),
            search_value: String::new(),
            search_regex: false,
            pages: 3,
            pagelength: 25,
        }
    }

    fn to_query_string(&self) -> String {
        let mut params = vec![
            format!("draw={}", self.draw),
            format!("start={}", self.start),
            format!("length={}", self.length),
        ];

        // Add column parameters
        for (i, col) in self.columns.iter().enumerate() {
            params.push(format!("columns[{}][data]={}", i, col.data));
            params.push(format!("columns[{}][name]={}", i, col.name));
            params.push(format!("columns[{}][searchable]={}", i, col.searchable));
            params.push(format!("columns[{}][orderable]={}", i, col.orderable));
            params.push(format!(
                "columns[{}][search][value]={}",
                i, col.search_value
            ));
            params.push(format!(
                "columns[{}][search][regex]={}",
                i, col.search_regex
            ));
        }

        // Add ordering parameters
        params.push(format!("order[0][column]={}", self.order_column));
        params.push(format!("order[0][dir]={}", self.order_dir));
        params.push(format!("order[0][name]={}", self.order_name));

        // Add search parameters
        params.push(format!("search[value]={}", self.search_value));
        params.push(format!("search[regex]={}", self.search_regex));

        // Add pagination parameters
        params.push(format!("pages={}", self.pages));
        params.push(format!("pagelength={}", self.pagelength));

        params.join("&")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScraperState {
    last_processed_timestamp: DateTime<Utc>,
    total_processed: u64,
    last_run: DateTime<Utc>,
}

struct Scraper {
    client: reqwest::Client,
    quest_sender: Sender,
    seen_urls: HashSet<String>,
    rate_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    stats: Arc<Mutex<ScraperStats>>,
    metrics: MetricsCollector,
    run_all: bool,
}

impl Scraper {
    pub fn new(config: ScraperConfig, run_all: bool) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(&config.scraping.user_agent)
            .timeout(Duration::from_secs(config.scraping.request_timeout_secs))
            .build()
            .context("Failed to create HTTP client")?;

        let quest_sender = Sender::from_conf(&config.get_questdb_url())
            .context("Failed to create QuestDB sender")?;

        let seen_urls = HashSet::new();

        let quota = Quota::per_second(
            NonZeroU32::new(config.rate_limits.requests_per_second)
                .ok_or_else(|| anyhow!("Invalid requests_per_second value"))?,
        );
        let rate_limiter = Arc::new(RateLimiter::direct(quota));

        let stats = Arc::new(Mutex::new(ScraperStats {
            requests_per_second: 0.0,
            total_matches_found: 0,
            latest_update: None,
            current_page: 0,
            total_pages: 0,
            total_results: 0,
            status: "Initializing".to_string(),
            metrics: None,
        }));

        let metrics = MetricsCollector::new();

        Ok(Self {
            client,
            quest_sender,
            seen_urls,
            rate_limiter,
            stats,
            metrics,
            run_all,
        })
    }

    fn extract_match_urls(description: &str) -> Vec<String> {
        let re =
            Regex::new(r#"href="(https://www\.mkttl\.co\.uk/matches/team/\d+/\d+/\d+/\d+/\d+)""#)
                .expect("Invalid regex pattern");

        re.captures_iter(description)
            .map(|cap| cap[1].to_string())
            .collect()
    }

    async fn retry_with_backoff<F, Fut, T>(mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut delay = INITIAL_RETRY_DELAY;
        let mut attempt = 1;

        loop {
            match operation().await {
                Ok(value) => return Ok(value),
                Err(e) => {
                    if attempt >= MAX_RETRIES {
                        return Err(e.context("Max retries exceeded"));
                    }
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                    attempt += 1;
                }
            }
        }
    }

    fn validate_html(html: &str) -> bool {
        html.contains("div id=\"games\"")
    }

    async fn fetch_match_html(&self, url: &str) -> Result<String> {
        info!("Fetching HTML for match URL: {}", url);

        Self::retry_with_backoff(|| async {
            let response = self.client.get(url).send().await?;

            if !response.status().is_success() {
                anyhow::bail!("Failed to fetch HTML: HTTP {}", response.status());
            }

            let html = response.text().await?;
            info!(
                "Successfully downloaded HTML ({} bytes) for {}",
                html.len(),
                url
            );

            // Validate HTML
            if !Self::validate_html(&html) {
                anyhow::bail!("Invalid HTML - missing games div");
            }
            info!("Successfully validated HTML for {}", url);

            Ok(html)
        })
        .await
    }

    async fn fetch_event_log_page(
        &mut self,
        page: u32,
        page_length: u32,
    ) -> Result<EventLogResponse> {
        // Wait for rate limiter and record wait time
        let wait_start = Instant::now();
        self.rate_limiter.until_ready().await;
        let wait_duration = wait_start.elapsed();
        self.metrics.record_rate_limit_wait(wait_duration);

        let params = DataTablesParams::new(page, page_length);
        let url = format!("{}?{}", EVENT_LOG_BASE_URL, params.to_query_string());

        info!("Fetching page {} with {} records", page, page_length);

        // Update stats
        {
            let mut stats = self.stats.lock().unwrap();
            stats.current_page = page;
            stats.status = format!("Fetching page {}", page);
            stats.metrics = Some(self.metrics.get_metrics());
        }

        let client = self.client.clone();
        let tracker = self.metrics.record_request_start();

        let response = Self::retry_with_backoff(|| async {
            let response = client
                .get(&url)
                .header("Accept", "application/json, text/javascript, */*; q=0.01")
                .send()
                .await
                .context("Failed to fetch event log")?;

            let status = response.status();
            let headers = response.headers().clone();
            let text = response
                .text()
                .await
                .context("Failed to get response text")?;

            if !status.is_success() {
                error!("Request failed with status {}", status);
                error!("Response headers: {:?}", headers);
                error!("Response body: {}", text);

                // Update stats with error
                let mut stats = self.stats.lock().unwrap();
                stats.status = format!("Error: Request failed with status {}", status);
                stats.metrics = Some(self.metrics.get_metrics());

                anyhow::bail!("Request failed with status {}", status);
            }

            serde_json::from_str::<EventLogResponse>(&text)
                .with_context(|| format!("Failed to parse response: {}", text))
        })
        .await;

        match &response {
            Ok(_) => tracker.finish(true),
            Err(e) => {
                tracker.finish(false);
                self.metrics.record_error(e.to_string());
            }
        }

        let response = response?;

        // Update stats
        let mut stats = self.stats.lock().unwrap();
        stats.total_pages = (response.records_total + page_length - 1) / page_length;
        stats.metrics = Some(self.metrics.get_metrics());

        Ok(response)
    }

    async fn get_existing_urls(&self) -> Result<HashSet<String>> {
        info!("Querying existing URLs from QuestDB");
        let query = "SELECT DISTINCT url FROM mkttl_matches";
        let quest_query_url = format!(
            "http://localhost:9000/exec?query={}",
            urlencoding::encode(query)
        );

        let response = self
            .client
            .get(&quest_query_url)
            .send()
            .await
            .context("Failed to query QuestDB")?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to query QuestDB: HTTP {}", response.status());
        }

        let json: serde_json::Value = response
            .json()
            .await
            .context("Failed to parse QuestDB response")?;

        let urls: HashSet<String> = json["dataset"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|row| row[0].as_str().map(String::from))
            .collect();

        info!("Found {} existing URLs in QuestDB", urls.len());
        Ok(urls)
    }

    async fn store_match_events(&mut self, events: Vec<MatchEvent>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        info!("Processing {} match events in bulk", events.len());
        let mut buffer = Buffer::new();
        let mut successful_events = 0;

        for event in events {
            // Fetch HTML for the match URL
            match self.fetch_match_html(&event.url).await {
                Ok(html) => {
                    buffer
                        .table("mkttl_matches")?
                        .symbol("updated_by", &event.updated_by)?
                        .column_str("url", &event.url)?
                        .column_str("raw_html", &html)?
                        .column_ts("fetched_at", TimestampNanos::now())?
                        .at(TimestampNanos::new(event.timestamp.timestamp_nanos_opt().unwrap()))?;

                    successful_events += 1;
                }
                Err(e) => {
                    error!("Failed to fetch HTML for {}: {}", event.url, e);
                    continue;
                }
            }
        }

        if successful_events > 0 {
            self.quest_sender.flush(&mut buffer)?;

            // Update stats
            let mut stats = self.stats.lock().unwrap();
            stats.total_matches_found += successful_events as u64;
            stats.latest_update = Some(Utc::now());

            info!("Stored {} match events successfully", successful_events);
        }

        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        // First get existing URLs from QuestDB
        let existing_urls = self.get_existing_urls().await?;
        self.seen_urls = existing_urls;

        let mut page = 0;
        let mut pending_events = Vec::new();

        // Get first page to determine total results
        let first_response = self.fetch_event_log_page(0, 2).await?;
        let total_results = first_response.records_total;
        let total_pages = (total_results + FULL_SCRAPE_PAGE_SIZE - 1) / FULL_SCRAPE_PAGE_SIZE;
        
        // Update stats with total results
        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_results = total_results;
            stats.total_pages = total_pages;
        }

        info!("Total results: {}, Total pages: {}", total_results, total_pages);

        loop {
            // Use smaller page size for first page, then switch to full size
            let page_size = if page == 0 { 2 } else { FULL_SCRAPE_PAGE_SIZE };
            
            let response = if page == 0 {
                first_response.clone()
            } else {
                self.fetch_event_log_page(page, page_size).await?
            };

            if response.data.is_empty() {
                info!("No more data found, stopping scrape");
                break;
            }

            let mut found_existing = false;
            for row in response.data {
                if row.len() < 4 {
                    error!("Invalid event log row: {:?}", row);
                    continue;
                }

                let username = &row[0];
                let timestamp_str = &row[1];
                let category = &row[2];
                let description = &row[3];

                if category != "Matches" {
                    continue;
                }

                // Parse timestamp
                let timestamp =
                    match chrono::NaiveDateTime::parse_from_str(timestamp_str, "%d/%m/%Y %H:%M:%S")
                    {
                        Ok(ts) => ts.and_utc(),
                        Err(e) => {
                            error!("Failed to parse timestamp {}: {}", timestamp_str, e);
                            continue;
                        }
                    };

                // Extract username from HTML link
                let username = if username.contains("href") {
                    let re = Regex::new(r#">([^<]+)</a>"#).expect("Invalid regex pattern");
                    re.captures(username)
                        .and_then(|cap| cap.get(1))
                        .map(|m| m.as_str().to_string())
                        .unwrap_or_else(|| username.to_string())
                } else {
                    username.to_string()
                };

                let match_urls = Self::extract_match_urls(description);
                for url in match_urls {
                    if self.seen_urls.contains(&url) {
                        info!("Found existing URL: {}", url);
                        if !self.run_all {
                            info!("Stopping scrape (run_all=false)");
                            found_existing = true;
                            break;
                        }
                        continue;
                    }

                    self.seen_urls.insert(url.clone());
                    pending_events.push(MatchEvent {
                        timestamp,
                        url,
                        updated_by: username.clone(),
                    });

                    // Process in batches of 10
                    if pending_events.len() >= 10 {
                        self.store_match_events(pending_events.drain(..).collect())
                            .await?;
                    }
                }

                if found_existing {
                    break;
                }
            }

            // If we found an existing URL and run_all is false, stop
            if found_existing && !self.run_all {
                info!("Found existing URLs, stopping scrape (run_all=false)");
                break;
            }

            page += 1;
            
            // Update progress in stats
            {
                let mut stats = self.stats.lock().unwrap();
                stats.current_page = page;
                stats.status = format!("Processing page {} of {}", page, total_pages);
            }

            // If we've processed all pages, stop
            if page >= total_pages as u32 {
                info!("Processed all pages, stopping scrape");
                break;
            }
        }

        // Process any remaining events
        if !pending_events.is_empty() {
            self.store_match_events(pending_events).await?;
        }

        Ok(())
    }

    pub fn get_app_state(&self) -> AppState {
        AppState {
            stats: self.stats.clone(),
            shutdown_tx: Arc::new(Mutex::new(None)),
        }
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        error!("Please specify a scraper type: 'events', 'html', 'games', or 'web'");
        std::process::exit(1);
    }

    let scraper_type = &args[1];
    let run_all = args.len() > 2 && args[2] == "runAll";
    let config = ScraperConfig::from_env();

    match scraper_type.as_str() {
        "events" => {
            // Create a runtime for the async event scraper
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                info!("Starting MKTTL match card event scraper");
                let mut scraper = Scraper::new(config.clone(), run_all)?;
                let app_state = scraper.get_app_state();

                // Start web server and get shutdown signal receiver
                let shutdown_rx = web::serve(app_state).await;

                // Run the scraper until shutdown signal
                tokio::select! {
                    result = scraper.run() => {
                        if let Err(e) = result {
                            error!("Scraper error in events scraper: {}", e);
                        }
                    }
                    _ = shutdown_rx => {
                        info!("Received shutdown signal");
                    }
                    _ = signal::ctrl_c() => {
                        info!("Received Ctrl+C signal");
                    }
                }
                Ok::<(), anyhow::Error>(())
            })?;
        }
        "html" => {
            info!("Starting MKTTL match HTML scraper");
            let mut scraper = MatchHtmlScraper::new(config.clone())?;

            // Run the HTML scraper synchronously
            if let Err(e) = scraper.run() {
                error!("Scraper error in html scraper: {}", e);
            }
        }
        "games" => {
            info!("Starting game data scraper");
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let mut game_scraper = GameScraper::new(&config)?;
                if let Err(e) = game_scraper.run().await {
                    error!("Error in game scraper: {}", e);
                }
                Ok::<(), anyhow::Error>(())
            })?;
        }
        "web" => {
            info!("Starting web server only");
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let app_state = AppState {
                    stats: Arc::new(Mutex::new(ScraperStats::default())),
                    shutdown_tx: Arc::new(Mutex::new(None)),
                };

                let shutdown_rx = web::serve(app_state).await;
                info!("Web server started at http://127.0.0.1:3000");

                tokio::select! {
                    _ = shutdown_rx => {
                        info!("Received shutdown signal");
                    }
                    _ = signal::ctrl_c() => {
                        info!("Received Ctrl+C signal");
                    }
                }
                Ok::<(), anyhow::Error>(())
            })?;
        }
        _ => {
            error!("Invalid scraper type. Use 'events', 'html', 'games', or 'web'");
            std::process::exit(1);
        }
    }

    Ok(())
}

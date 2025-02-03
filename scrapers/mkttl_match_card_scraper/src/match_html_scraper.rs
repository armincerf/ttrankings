use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    fs,
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use tracing::{error, info};
use serde_json;
use regex;
use scraper;

use crate::config::ScraperConfig;

const CONCURRENT_REQUESTS: u32 = 4;
const MAX_RETRIES: u32 = 2;
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(1);
const HTML_FILES_DIR: &str = "html_files";
const URLS_FILE: &str = "html_files/urls.txt";
const STATE_FILE: &str = "html_files/state.json";
const CACHE_DIR: &str = "html_files/cache";
const INITIAL_PAGE_SIZE: u32 = 3;
const SUBSEQUENT_PAGE_SIZE: u32 = 200;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UrlState {
    last_checked: DateTime<Utc>,
    last_modified: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScraperState {
    urls: HashMap<String, UrlState>,
    last_run: DateTime<Utc>,
    is_initial_run: bool,
}

impl Default for ScraperState {
    fn default() -> Self {
        Self {
            urls: HashMap::new(),
            last_run: Utc::now(),
            is_initial_run: true,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MatchHtmlStats {
    pub requests_per_second: f64,
    pub total_matches_processed: u64,
    pub total_matches_to_process: usize,
    pub latest_update: Option<DateTime<Utc>>,
    pub status: String,
    pub total_match_urls: usize,
}

pub struct MatchHtmlScraper {
    client: reqwest::blocking::Client,
    rate_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    stats: Arc<Mutex<MatchHtmlStats>>,
    state: ScraperState,
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
            total_match_urls: 0,
        }));

        let quota = Quota::per_second(
            NonZeroU32::new(config.rate_limits.requests_per_second)
                .ok_or_else(|| anyhow!("Invalid requests_per_second value"))?,
        );
        let rate_limiter = Arc::new(RateLimiter::direct(quota));

        // Load state from file or create new
        let state = if Path::new(STATE_FILE).exists() {
            let state_json = fs::read_to_string(STATE_FILE)?;
            serde_json::from_str(&state_json).unwrap_or_default()
        } else {
            ScraperState::default()
        };

        Ok(Self {
            client,
            rate_limiter,
            stats,
            state,
        })
    }

    fn save_state(&self) -> Result<()> {
        fs::create_dir_all(HTML_FILES_DIR)?;
        let state_json = serde_json::to_string_pretty(&self.state)?;
        fs::write(STATE_FILE, state_json)?;
        Ok(())
    }

    fn validate_html(html: &str) -> bool {
        // Parse the HTML
        let document = scraper::Html::parse_document(html);
        // Look for h2 elements
        let h2_selector = scraper::Selector::parse("h2").unwrap();
        if let Some(h2) = document.select(&h2_selector).next() {
            let text = h2.text().collect::<String>();
            return text == "League" || text == "Challenge Cup";
        }
        false
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

    fn url_to_filepath(url: &str) -> PathBuf {
        // Extract just the numeric parts of the URL
        let re = regex::Regex::new(r"/matches/team/(\d+/\d+/\d+/\d+/\d+)").unwrap();
        let safe_filename = if let Some(cap) = re.captures(url) {
            cap[1].replace('/', "_")
        } else {
            url.replace('/', "_")
        };
        Path::new(HTML_FILES_DIR).join(format!("{}.html", safe_filename))
    }

    fn parse_last_modified(desc_str: &str) -> Option<DateTime<Utc>> {
        // Extract timestamp from description like "31/01/2025 23:13:08"
        let re = regex::Regex::new(r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})").unwrap();
        if let Some(cap) = re.captures(desc_str) {
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&cap[1], "%d/%m/%Y %H:%M:%S") {
                return Some(dt.and_utc());
            }
        }
        None
    }

    fn get_cache_path(start_record: u32, end_record: u32) -> PathBuf {
        Path::new(CACHE_DIR).join(format!("records_{}_to_{}.json", start_record, end_record))
    }

    fn query_event_viewer(&self, start: u32, length: u32, total_records: Option<u32>) -> Result<serde_json::Value> {
        info!("Querying event viewer API (start: {}, length: {})", start, length);
        
        // Create cache directory if it doesn't exist
        fs::create_dir_all(CACHE_DIR)?;
        
        // Calculate the record range for this request
        // Since records are returned newest first:
        // - start=0, length=200 means records total-0-(200-1) to total-0 (newest)
        // - start=200, length=200 means records total-200-(200-1) to total-200
        // - and so on until we reach 0
        let end_record = if let Some(total) = total_records {
            total - start
        } else {
            0 // Will be updated after API call
        };
        
        let start_record = if end_record >= length {
            end_record - length
        } else {
            0
        };
        
        let cache_path = Self::get_cache_path(start_record, end_record);
        
        // Try to read from cache first
        if cache_path.exists() {
            info!("Found cached response for records {} to {}", start_record, end_record);
            let cached_json = fs::read_to_string(&cache_path)?;
            return Ok(serde_json::from_str(&cached_json)?);
        }

        info!("Fetching records {} to {} from API", start_record, end_record);

        // If not in cache, query the API
        let query_url = "https://www.mkttl.co.uk/event-viewer/load.js";
        let response = self.client
            .get(query_url)
            .header("Accept", "application/json, text/javascript, */*; q=0.01")
            .query(&serde_json::json!({
                "draw": "2",
                "columns[0][data]": "0",
                "columns[0][name]": "user.username",
                "columns[0][searchable]": "true",
                "columns[0][orderable]": "true",
                "columns[0][search][value]": "",
                "columns[0][search][regex]": "false",
                "columns[1][data]": "1",
                "columns[1][name]": "me.log_updated",
                "columns[1][searchable]": "true",
                "columns[1][orderable]": "true",
                "columns[1][search][value]": "",
                "columns[1][search][regex]": "false",
                "columns[2][data]": "2",
                "columns[2][name]": "system_event_log_type.object_description",
                "columns[2][searchable]": "true",
                "columns[2][orderable]": "true",
                "columns[2][search][value]": "",
                "columns[2][search][regex]": "false",
                "columns[3][data]": "3",
                "columns[3][name]": "description",
                "columns[3][searchable]": "true",
                "columns[3][orderable]": "false",
                "columns[3][search][value]": "",
                "columns[3][search][regex]": "false",
                "order[0][column]": "1",
                "order[0][dir]": "desc",
                "order[0][name]": "me.log_updated",
                "start": start,
                "length": length,
                "search[value]": "",
                "search[regex]": "false",
                "pages": "3",
                "pagelength": length
            }))
            .send()?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to query event viewer API: HTTP {}", response.status());
        }

        let text = response.text()?;
        info!("Got response from event viewer API ({} bytes)", text.len());
        
        let json: serde_json::Value = serde_json::from_str(&text)?;
        
        // If we don't have total_records, get it from the response
        let total_records = total_records.unwrap_or_else(|| {
            json.get("recordsTotal")
                .and_then(|v| v.as_u64())
                .map(|v| v as u32)
                .unwrap_or(0)
        });
        
        // Now we can calculate the actual record range
        let end_record = total_records - start;
        let start_record = if end_record >= length {
            end_record - length
        } else {
            0
        };
        
        // Cache the response with the correct record range
        let cache_path = Self::get_cache_path(start_record, end_record);
        fs::write(&cache_path, serde_json::to_string_pretty(&json)?)?;
        info!("Cached API response to {:?} (records {} to {})", cache_path, start_record, end_record);
        
        Ok(json)
    }

    fn process_api_response(&self, json: serde_json::Value) -> Result<Vec<(String, DateTime<Utc>)>> {
        let mut new_urls = Vec::new();

        if let Some(data) = json.get("data") {
            if let Some(array) = data.as_array() {
                for item in array {
                    if let (Some(description), Some(timestamp_str)) = (item.get(3), item.get(1)) {
                        if let (Some(desc_str), Some(ts_str)) = (description.as_str(), timestamp_str.as_str()) {
                            let mut search_start = 0;
                            while let Some(start) = desc_str[search_start..].find("/matches/team/") {
                                let start = search_start + start;
                                if let Some(end) = desc_str[start..].find("\"") {
                                    let url = format!("https://www.mkttl.co.uk{}", &desc_str[start..start + end]);
                                    let filepath = Self::url_to_filepath(&url);
                                    
                                    if let Some(last_modified) = Self::parse_last_modified(ts_str) {
                                        let needs_update = if let Some(state) = self.state.urls.get(&url) {
                                            last_modified > state.last_modified
                                        } else {
                                            true
                                        };

                                        // Only add URL if file doesn't exist or needs update
                                        if needs_update && (!filepath.exists() || {
                                            // If file exists but is empty or corrupted, redownload
                                            if let Ok(metadata) = filepath.metadata() {
                                                metadata.len() == 0
                                            } else {
                                                true
                                            }
                                        }) {
                                            new_urls.push((url.clone(), last_modified));
                                        }
                                    }
                                    search_start = start + end;
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(new_urls)
    }

    fn gather_urls(&mut self) -> Result<Vec<String>> {
        info!("Gathering match URLs from event viewer API");
        let mut stats = self.stats.lock().unwrap();
        stats.status = "Querying event viewer API for match URLs...".to_string();

        fs::create_dir_all(HTML_FILES_DIR)?;

        let mut all_new_urls = Vec::new();
        let mut start = 0;
        let page_size = SUBSEQUENT_PAGE_SIZE;
        info!("Using page size of {}", page_size);

        // Get total records from first request
        let json = self.query_event_viewer(0, 1, None)?;
        let total_records = json.get("recordsTotal")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32)
            .ok_or_else(|| anyhow!("Failed to get total records"))?;
        
        info!("Total records available: {} (expecting {} pages)", 
              total_records, 
              (total_records as f64 / page_size as f64).ceil());

        let mut page_number = 1;
        loop {
            info!("Fetching page {} (offset: {}, page size: {})", page_number, start, page_size);
            // Query the API with total_records
            let json = self.query_event_viewer(start, page_size, Some(total_records))?;

            // Process the response
            let new_urls = self.process_api_response(json)?;
            let new_urls_count = new_urls.len();
            info!("Found {} new/updated URLs on page {}", new_urls_count, page_number);
            
            // Update state and collect URLs
            for (url, last_modified) in new_urls {
                let is_new = !self.state.urls.contains_key(&url);
                self.state.urls.insert(url.clone(), UrlState {
                    last_checked: Utc::now(),
                    last_modified,
                });
                all_new_urls.push(url.clone());
                
                if is_new {
                    info!("New match found: {} (modified: {})", url, last_modified);
                } else {
                    info!("Updated match found: {} (modified: {})", url, last_modified);
                }
            }

            start += page_size;
            page_number += 1;

            // Check if we've processed all records
            if start >= total_records {
                info!("Reached end of available records after {} pages", page_number - 1);
                break;
            }
            info!("Progress: processed {}/{} records ({}%)", 
                  start.min(total_records), 
                  total_records,
                  (start.min(total_records) as f64 / total_records as f64 * 100.0) as u32);

            // Rate limiting
            thread::sleep(Duration::from_millis(100));
            
            // Save state periodically (every 5 pages)
            if page_number % 5 == 0 {
                info!("Saving state after {} pages (found {} URLs so far)", 
                      page_number, all_new_urls.len());
                self.save_state()?;
            }
        }

        info!("Found {} new match URLs to process", all_new_urls.len());
        stats.total_matches_to_process = all_new_urls.len();
        stats.status = format!("Found {} URLs to process", all_new_urls.len());

        // Write URLs to file
        fs::write(URLS_FILE, all_new_urls.join("\n"))?;
        info!("Wrote {} URLs to {}", all_new_urls.len(), URLS_FILE);

        // Update state
        if self.state.is_initial_run {
            info!("Completed initial run - setting is_initial_run to false");
            self.state.is_initial_run = false;
        }
        self.save_state()?;

        Ok(all_new_urls)
    }

    fn validate_cache_coverage(&self) -> Result<()> {
        info!("Validating cache coverage...");
        
        // Get all cache files
        let cache_files = fs::read_dir(CACHE_DIR)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path()
                    .extension()
                    .map(|ext| ext == "json")
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
            
        info!("Found {} cache files", cache_files.len());
        
        // Collect all unique match URLs from cache files
        let mut unique_urls = std::collections::HashSet::new();
        let mut total_records = 0;
        
        for entry in cache_files {
            let content = fs::read_to_string(entry.path())?;
            let json: serde_json::Value = serde_json::from_str(&content)?;
            
            if let Some(data) = json.get("data").and_then(|d| d.as_array()) {
                total_records += data.len();
                for item in data {
                    if let Some(description) = item.get(3).and_then(|d| d.as_str()) {
                        if description.contains("/matches/team/") {
                            if let Some(start) = description.find("/matches/team/") {
                                if let Some(end) = description[start..].find("\"") {
                                    let url = format!("https://www.mkttl.co.uk{}", &description[start..start + end]);
                                    unique_urls.insert(url);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        info!("Found {} total records in cache", total_records);
        info!("Found {} unique match URLs in cache", unique_urls.len());
        
        // Get all HTML files
        let html_files = fs::read_dir(HTML_FILES_DIR)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path()
                    .extension()
                    .map(|ext| ext == "html")
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
            
        info!("Found {} HTML files", html_files.len());
        
        // Check for missing files
        let mut missing_files = Vec::new();
        for url in &unique_urls {
            let filepath = Self::url_to_filepath(url);
            if !filepath.exists() {
                missing_files.push(url.clone());
            }
        }
        
        // Check for orphaned files (HTML files without corresponding URL in cache)
        let mut orphaned_files = Vec::new();
        for entry in html_files {
            let path = entry.path();
            let filename = path.file_stem().unwrap().to_string_lossy();
            // Convert filename back to URL format
            let parts: Vec<&str> = filename.split('_').collect();
            if parts.len() == 5 {
                let url = format!(
                    "https://www.mkttl.co.uk/matches/team/{}/{}/{}/{}/{}",
                    parts[0], parts[1], parts[2], parts[3], parts[4]
                );
                if !unique_urls.contains(&url) {
                    orphaned_files.push(path.to_string_lossy().to_string());
                }
            }
        }
        
        // Report results
        if missing_files.is_empty() && orphaned_files.is_empty() {
            info!("Cache validation successful! All files are present and accounted for.");
        } else {
            if !missing_files.is_empty() {
                info!("Found {} missing HTML files:", missing_files.len());
                for url in missing_files {
                    info!("Missing: {}", url);
                }
            }
            if !orphaned_files.is_empty() {
                info!("Found {} orphaned HTML files (no corresponding URL in cache):", orphaned_files.len());
                for file in orphaned_files {
                    info!("Orphaned: {}", file);
                }
            }
        }
        
        Ok(())
    }

    pub fn run(&mut self, limit: Option<usize>) -> Result<()> {
        info!("Starting HTML scraper");

        // Step 1: Gather URLs that need updating
        let mut urls = self.gather_urls()?;
        
        // Validate cache coverage
        self.validate_cache_coverage()?;
        
        // Apply limit if specified
        if let Some(limit) = limit {
            urls.truncate(limit);
            info!("Limited to processing {} URLs", limit);
        }

        // Step 2: Process URLs with workers
        let queue = Arc::new(Mutex::new(VecDeque::from(urls)));
        info!("Starting {} concurrent workers", CONCURRENT_REQUESTS);

        let mut handles = Vec::new();
        for i in 0..CONCURRENT_REQUESTS {
            let queue = Arc::clone(&queue);
            let rate_limiter = Arc::clone(&self.rate_limiter);
            let client = self.client.clone();
            let stats = Arc::clone(&self.stats);

            let handle = thread::spawn(move || -> Result<()> {
                info!("Worker {} started", i);
                while let Some(url) = {
                    let mut queue = queue.lock().unwrap();
                    queue.pop_front()
                } {
                    info!("Worker {} processing URL: {}", i, url);
                    let filepath = Self::url_to_filepath(&url);

                    {
                        let mut stats = stats.lock().unwrap();
                        stats.status = format!("Worker {} processing URL: {}", i, url);
                    }

                    // Wait for rate limiter
                    while let Err(_) = rate_limiter.check() {
                        thread::sleep(Duration::from_millis(100));
                    }

                    let result = Self::retry_with_backoff(|| {
                        info!("Worker {} fetching HTML from {}", i, url);
                        let response = client.get(&url).send()?;

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
                            anyhow::bail!("Invalid HTML - missing League or Challenge Cup h2 element");
                        }
                        info!("Worker {} successfully validated HTML for {}", i, url);

                        // Write HTML to file
                        fs::write(&filepath, html)?;
                        info!("Worker {} wrote HTML to {:?}", i, filepath);

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

        // Update final status and save state
        {
            let mut stats = self.stats.lock().unwrap();
            stats.status = format!(
                "Completed processing {} URLs",
                stats.total_matches_processed
            );
        }
        self.state.last_run = Utc::now();
        self.save_state()?;

        Ok(())
    }
}

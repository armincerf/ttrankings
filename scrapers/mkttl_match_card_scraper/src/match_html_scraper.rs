use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use tracing::{error, info};
use serde_json;
use regex;
use scraper;
use html_escape;
use indicatif::{ProgressBar, ProgressStyle};

use crate::config::ScraperConfig;

const HTML_FILES_DIR: &str = "/Users/alexdavis/ghq/github.com/armincerf/ttrankings/scrapers/mkttl_match_card_scraper/html_files";
const URLS_FILE: &str = "/Users/alexdavis/ghq/github.com/armincerf/ttrankings/scrapers/mkttl_match_card_scraper/html_files/urls.txt";
const CACHE_DIR: &str = "/Users/alexdavis/ghq/github.com/armincerf/ttrankings/scrapers/mkttl_match_card_scraper/html_files/cache";
const LAST_RUN_FILE: &str = "/Users/alexdavis/ghq/github.com/armincerf/ttrankings/scrapers/mkttl_match_card_scraper/html_files/last_run";
const PAGE_SIZE: u32 = 100;
const CONCURRENT_REQUESTS: usize = 4;
const MAX_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY: u64 = 1000;

#[derive(Debug, Clone, Serialize)]
pub struct MatchHtmlStats {
    pub total_matches_processed: u64,
    pub total_matches_to_process: usize,
    pub latest_update: Option<DateTime<Utc>>,
    pub status: String,
    pub total_match_urls: usize,
    pub errors: Vec<String>,
}

impl Default for MatchHtmlStats {
    fn default() -> Self {
        Self {
            total_matches_processed: 0,
            total_matches_to_process: 0,
            latest_update: None,
            status: "Initializing".to_string(),
            total_match_urls: 0,
            errors: Vec::new(),
        }
    }
}

pub struct MatchHtmlScraper {
    client: reqwest::blocking::Client,
    stats: Arc<Mutex<MatchHtmlStats>>,
    script_start_time: DateTime<Utc>,
    progress_bar: Option<ProgressBar>,
}

impl MatchHtmlScraper {
    pub fn new(_config: ScraperConfig) -> Result<Self> {
        let client = reqwest::blocking::Client::new();
        let stats = Arc::new(Mutex::new(MatchHtmlStats::default()));
        let script_start_time = Utc::now();

        Ok(Self {
            client,
            stats,
            script_start_time,
            progress_bar: None,
        })
    }

    pub fn with_date(mut self, date_str: &str) -> Result<Self> {
        // Parse date string in format DD/MM/YYYY
        let dt = chrono::NaiveDateTime::parse_from_str(&format!("{} 00:00:00", date_str), "%d/%m/%Y %H:%M:%S")?;
        self.script_start_time = dt.and_utc();
        Ok(self)
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

    fn url_to_filepath(url: &str) -> PathBuf {
        // Extract just the numeric parts of the URL
        let re = regex::Regex::new(r"/matches/team/([^/]+/[^/]+/[^/]+/[^/]+/[^/]+)").unwrap();
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

    fn query_event_viewer(&self, start: u32, length: u32) -> Result<serde_json::Value> {
        // Create cache directory if it doesn't exist
        fs::create_dir_all(CACHE_DIR)?;
        
        // Calculate the record range for this request
        let end_record = start + length;
        
        let cache_path = Self::get_cache_path(start, end_record);
        
        // Try to read from cache first if length greater than 1
        if cache_path.exists() && length > 1 {
            let cached_json = fs::read_to_string(&cache_path)?;
            return Ok(serde_json::from_str(&cached_json)?);
        }

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
                "order[0][dir]": "asc",
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
        
        let json: serde_json::Value = serde_json::from_str(&text)?;
        
        info!("API Response - recordsTotal: {}, recordsFiltered: {}, start: {}, length: {}", 
            json.get("recordsTotal").and_then(|v| v.as_u64()).unwrap_or(0),
            json.get("recordsFiltered").and_then(|v| v.as_u64()).unwrap_or(0),
            start,
            length
        );
        
        // Cache the response
        fs::write(&cache_path, serde_json::to_string_pretty(&json)?)?;
        
        Ok(json)
    }

    fn process_api_response(&self, json: serde_json::Value) -> Result<Vec<(String, DateTime<Utc>)>> {
        let mut new_urls = Vec::new();
        let url_regex = regex::Regex::new(r#"https://www\.mkttl\.co\.uk/matches/team/[^/'"]+/[^/'"]+/[^/'"]+/[^/'"]+/[^/'"<>]+"#)
            .expect("Failed to compile regex to match team URLs");

        if let Some(data) = json.get("data") {
            if let Some(array) = data.as_array() {
                for item in array {
                    let desc_str = item.get(3).and_then(|v| v.as_str()).unwrap_or("");
                    let ts_str = item.get(1).and_then(|v| v.as_str()).unwrap_or("");
                    let decoded_desc = html_escape::decode_html_entities(desc_str);

                    // Only process URLs that contain "2025"
                    if decoded_desc.contains("/2025/") {
                        if let Some(last_modified) = Self::parse_last_modified(ts_str) {
                            for cap in url_regex.find_iter(&decoded_desc) {
                                let url = cap.as_str().to_string();
                                new_urls.push((url.clone(), last_modified));
                            }
                        }
                    }
                }
            }
        }

        Ok(new_urls)
    }

    fn get_last_run_time() -> Option<DateTime<Utc>> {
        if let Ok(content) = fs::read_to_string(LAST_RUN_FILE) {
            if let Ok(timestamp) = content.parse::<i64>() {
                return Some(DateTime::from_timestamp(timestamp, 0).unwrap());
            }
        }
        None
    }

    fn update_last_run_time(&self) -> Result<()> {
        fs::create_dir_all(HTML_FILES_DIR)?;
        fs::write(LAST_RUN_FILE, self.script_start_time.timestamp().to_string())?;
        Ok(())
    }

    fn gather_urls(&mut self) -> Result<()> {
        let mut all_new_urls = Vec::new();
        fs::create_dir_all(HTML_FILES_DIR)?;
        fs::create_dir_all(CACHE_DIR)?;

        // First query to get current total records
        let initial_response = self.query_event_viewer(0, 1)?;
        let current_total_records = initial_response
            .get("recordsTotal")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32)
            .ok_or_else(|| anyhow!("Failed to get total records count"))?;

        info!("Initial query shows total records: {}", current_total_records);

        // First collect all existing cache files and their ranges
        let mut cached_ranges = Vec::new();
        if let Ok(entries) = fs::read_dir(CACHE_DIR) {
            for entry in entries.filter_map(Result::ok) {
                if let Some(filename) = entry.file_name().to_str() {
                    if let Some(captures) = regex::Regex::new(r"records_(\d+)_to_(\d+)\.json")?.captures(filename) {
                        if let (Some(start), Some(end)) = (captures.get(1), captures.get(2)) {
                            if let (Ok(start_record), Ok(end_record)) = (start.as_str().parse::<u32>(), end.as_str().parse::<u32>()) {
                                // Verify the cache file has valid content
                                let cache_path = entry.path();
                                if let Ok(content) = fs::read_to_string(&cache_path) {
                                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) {
                                        if let Some(data) = json.get("data").and_then(|d| d.as_array()) {
                                            if !data.is_empty() {
                                                cached_ranges.push((start_record, end_record));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Sort ranges by start record
        cached_ranges.sort_by_key(|&(start, _)| start);

        // Merge overlapping ranges
        let mut merged_ranges = Vec::new();
        if !cached_ranges.is_empty() {
            let mut current_range = cached_ranges[0];
            for &(start, end) in cached_ranges.iter().skip(1) {
                if start <= current_range.1 {  // If ranges overlap
                    // Merge them by taking the maximum end point
                    current_range.1 = end.max(current_range.1);
                } else if start == current_range.1 + 1 {  // If ranges are consecutive
                    // Extend the current range
                    current_range.1 = end;
                } else {
                    // Gap found, push current range and start new one
                    merged_ranges.push(current_range);
                    current_range = (start, end);
                }
            }
            // Don't forget to push the last range
            merged_ranges.push(current_range);
        }

        // Find gaps in the merged ranges
        let mut current_position = 0;
        let mut ranges_to_fetch = Vec::new();

        for (start, end) in merged_ranges {
            if current_position < start {
                ranges_to_fetch.push((current_position, start));
            }
            current_position = end;
        }

        // Add final range if needed
        if current_position < current_total_records {
            ranges_to_fetch.push((current_position, current_total_records));
        }

        if ranges_to_fetch.is_empty() {
            info!("No new event logs to process");
            return Ok(());
        }

        let total_records_to_fetch: u32 = ranges_to_fetch.iter().map(|(start, end)| end - start).sum();
        info!("Found {} ranges to fetch, total records: {}", ranges_to_fetch.len(), total_records_to_fetch);
        let total_pages = ((total_records_to_fetch as f64) / (PAGE_SIZE as f64)).ceil() as u64;
        
        let progress = ProgressBar::new(total_pages);
        progress.set_style(ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} pages processed ({eta})")
            .unwrap());
        progress.enable_steady_tick(Duration::from_millis(100));

        // Fetch each range in PAGE_SIZE chunks
        for (range_start, range_end) in ranges_to_fetch {
            let mut start = range_start;
            while start < range_end {
                let length = PAGE_SIZE.min(range_end - start);
                
                let response = self.query_event_viewer(start, length)?;
                let new_urls = self.process_api_response(response)?;
                for (url, _) in new_urls {
                    if !all_new_urls.contains(&url) {
                        all_new_urls.push(url.clone());
                    }
                }
                
                start += length;
                progress.inc(1);
            }
        }

        progress.finish_and_clear();

        let total_urls = all_new_urls.len();
        if total_urls > 0 {
            info!("Processing {} URLs from new event logs", total_urls);
            fs::write(URLS_FILE, all_new_urls.join("\n"))?;
            self.process_urls(all_new_urls)?;
        } else {
            info!("No new URLs found in event logs");
        }

        Ok(())
    }

    fn validate_cache_coverage(&self) -> Result<()> {
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
            info!("Cache validation successful: {} records, {} unique URLs", total_records, unique_urls.len());
        } else {
            info!("Cache validation found {} missing and {} orphaned files", missing_files.len(), orphaned_files.len());
        }
        
        Ok(())
    }

    fn retry_with_backoff<T, F>(mut f: F) -> Result<T>
    where
        F: FnMut() -> Result<T>,
    {
        let mut retries = 0;
        let mut delay = INITIAL_RETRY_DELAY;

        loop {
            match f() {
                Ok(value) => return Ok(value),
                Err(e) => {
                    if retries >= MAX_RETRIES {
                        return Err(e);
                    }
                    thread::sleep(Duration::from_millis(delay));
                    delay *= 2;
                    retries += 1;
                }
            }
        }
    }

    fn process_urls(&mut self, urls: Vec<String>) -> Result<()> {
        let queue: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::from(urls)));
        let progress = ProgressBar::new(queue.lock().unwrap().len() as u64);
        progress.set_style(ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} URLs processed ({eta})")
            .unwrap());

        let mut handles = Vec::new();
        
        for _ in 0..CONCURRENT_REQUESTS {
            let queue = Arc::clone(&queue);
            let client = self.client.clone();
            let stats = Arc::clone(&self.stats);
            let progress = progress.clone();

            let handle = thread::spawn(move || -> Result<()> {
                while let Some(url) = {
                    let mut queue = queue.lock().unwrap();
                    queue.pop_front()
                } {
                    let filepath = Self::url_to_filepath(&url);
                    let result = Self::retry_with_backoff(|| {
                        let response = client.get(&url).send()?;
                        let html = response.text()?;
                        if Self::validate_html(&html) {
                            fs::create_dir_all(HTML_FILES_DIR)?;
                            fs::write(&filepath, html)?;
                            Ok(())
                        } else {
                            Err(anyhow!("Invalid HTML content"))
                        }
                    });

                    match result {
                        Ok(_) => {
                            stats.lock().unwrap().total_matches_processed += 1;
                        }
                        Err(e) => {
                            error!("Failed to process {}: {}", url, e);
                            stats.lock().unwrap().errors.push(format!("Failed to process {}: {}", url, e));
                        }
                    }
                    progress.inc(1);
                }
                Ok(())
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap()?;
        }

        progress.finish();
        Ok(())
    }

    pub fn run(&mut self, _limit: Option<usize>) -> Result<()> {
        fs::create_dir_all(HTML_FILES_DIR)?;
        self.gather_urls()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::info;
    use test_log::test;

    #[test_log::test]
    fn test_process_api_response_with_tooltip_urls() -> Result<()> {
        let config = ScraperConfig::default();
        let scraper = MatchHtmlScraper::new(config)?;
        
        // Create test JSON with the complex HTML string
        let json = serde_json::json!({
            "data": [[
                "<a href=\"https://www.mkttl.co.uk/users/michaelh\">Michael Howard</a>",
                "13/12/2025 23:19:48",
                "Matches",
                "Updated the score for <a href=\"https://www.mkttl.co.uk/matches/team/18/104/2025/12/13\">Milton Keynes Phoenix Leighton Buzzard Generations</a>, <a href=\"https://www.mkttl.co.uk/matches/team/1/49/2025/12/13\">Chackmore Hasbeens Open University Primes</a>, <a href=\"https://www.mkttl.co.uk/matches/team/80/48/2025/12/13\">Woburn Sands Wolves Newport Pagnell Vanquish</a> and <a href=\"javascript:void(0)\" class=\"tip\" title=\"<a class='tip' href='https://www.mkttl.co.uk/matches/team/27/25/2025/12/13'>Milton Keynes Topspin Milton Keynes Pumas</a><br /><a class='tip' href='https://www.mkttl.co.uk/matches/team/21/20/2025/12/13'>Milton Keynes Spinners Milton Keynes Sasaki</a><br /><a class='tip' href='https://www.mkttl.co.uk/matches/team/74/45/2025/12/13'>Greenleys Knights Mursley Magpies</a><br /><a class='tip' href='https://www.mkttl.co.uk/matches/team/4/68/2025/12/13'>Greenleys Monarchs Greenleys Glory</a><br /><a class='tip' href='https://www.mkttl.co.uk/matches/team/84/96/2025/12/13'>Woburn Sands Hit and Miss Newport Pagnell Vantage</a>\">5 other matches</a>."
            ]]
        });

        // Expected URLs in the order they appear
        let expected_urls = vec![
            "https://www.mkttl.co.uk/matches/team/18/104/2025/12/13",
            "https://www.mkttl.co.uk/matches/team/1/49/2025/12/13",
            "https://www.mkttl.co.uk/matches/team/80/48/2025/12/13",
            "https://www.mkttl.co.uk/matches/team/27/25/2025/12/13",
            "https://www.mkttl.co.uk/matches/team/21/20/2025/12/13",
            "https://www.mkttl.co.uk/matches/team/74/45/2025/12/13",
            "https://www.mkttl.co.uk/matches/team/4/68/2025/12/13",
            "https://www.mkttl.co.uk/matches/team/84/96/2025/12/13",
        ];

        // Process the response
        let result = scraper.process_api_response(json)?;

        // Convert result URLs to a vector for comparison
        let result_urls: Vec<String> = result.into_iter().map(|(url, _)| url).collect();
        info!("Found {} URLs: {:?}", result_urls.len(), result_urls);

        // Check that we got all expected URLs
        assert_eq!(result_urls.len(), expected_urls.len(), "Wrong number of URLs extracted");
        
        // Check each expected URL is present
        for expected_url in expected_urls {
            assert!(
                result_urls.contains(&expected_url.to_string()),
                "Missing URL: {}",
                expected_url
            );
        }

        Ok(())
    }
}

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use html_escape;
use indicatif::{ProgressBar, ProgressStyle};
use regex;
use scraper;
use serde::Serialize;
use serde_json;
use std::{
    collections::{HashSet, VecDeque},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use tracing::{error, info};

use crate::config::ScraperConfig;
use crate::types::GameData;
use crate::utils::{detect_match_type, extract_teams_from_og_url};

const HTML_FILES_DIR: &str = "/Users/alexdavis/ghq/github.com/armincerf/ttrankings/scrapers/mkttl_match_card_scraper/html_files";
const URLS_FILE: &str = "/Users/alexdavis/ghq/github.com/armincerf/ttrankings/scrapers/mkttl_match_card_scraper/html_files/urls.txt";
const CACHE_DIR: &str = "/Users/alexdavis/ghq/github.com/armincerf/ttrankings/scrapers/mkttl_match_card_scraper/html_files/cache";
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

#[derive(Debug, Clone)]
pub struct RetryConfig {
    max_retries: u32,
    initial_delay: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: MAX_RETRIES,
            initial_delay: INITIAL_RETRY_DELAY,
        }
    }
}

pub struct MatchHtmlScraper {
    client: reqwest::blocking::Client,
    stats: Arc<Mutex<MatchHtmlStats>>,
}

impl MatchHtmlScraper {
    pub fn new(_config: ScraperConfig) -> Result<Self> {
        let client = reqwest::blocking::Client::new();
        let stats = Arc::new(Mutex::new(MatchHtmlStats::default()));

        Ok(Self { client, stats })
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

        // Try to read from cache first if length greater than 1 and not the initial query
        if cache_path.exists() && length > 1 {
            info!("Reading from cache file: {}", cache_path.display());
            let cached_json = fs::read_to_string(&cache_path)?;
            return Ok(serde_json::from_str(&cached_json)?);
        }

        // If not in cache, query the API
        let query_url = "https://www.mkttl.co.uk/event-viewer/load.js";
        let response = self
            .client
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
            anyhow::bail!(
                "Failed to query event viewer API: HTTP {}",
                response.status()
            );
        }

        let text = response.text()?;

        let json: serde_json::Value = serde_json::from_str(&text)?;

        info!(
            "API Response - recordsTotal: {}, recordsFiltered: {}, start: {}, length: {}",
            json.get("recordsTotal")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            json.get("recordsFiltered")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            start,
            length
        );

        // Only cache if not the initial query (length > 1)
        if length > 1 {
            info!("Caching response to: {}", cache_path.display());
            fs::write(&cache_path, serde_json::to_string_pretty(&json)?)?;
        }

        Ok(json)
    }

    fn retry_with_backoff<T, F>(&self, mut f: F, config: RetryConfig) -> Result<T>
    where
        F: FnMut() -> Result<T>,
    {
        let mut retries = 0;
        let mut delay = config.initial_delay;

        loop {
            match f() {
                Ok(value) => return Ok(value),
                Err(e) => {
                    if retries >= config.max_retries {
                        return Err(e);
                    }
                    thread::sleep(Duration::from_millis(delay));
                    delay *= 2;
                    retries += 1;
                }
            }
        }
    }

    fn process_api_response(
        &self,
        json: serde_json::Value,
    ) -> Result<HashSet<(String, DateTime<Utc>)>> {
        let mut new_urls = HashSet::new();
        let url_regex = regex::Regex::new(
            r#"https://www\.mkttl\.co\.uk/matches/team/[^/'"]+/[^/'"]+/[^/'"]+/[^/'"]+/[^/'"<>]+"#,
        )
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
                                new_urls.insert((cap.as_str().to_string(), last_modified));
                            }
                        }
                    }
                }
            }
        }

        Ok(new_urls)
    }

    fn extract_game_data(html: &str, url: &str, tx_time: DateTime<Utc>) -> Result<GameData> {
        let document = scraper::Html::parse_document(html);
        let match_type = detect_match_type(&document)?;
        let ((home_team_name, home_team_club), (away_team_name, away_team_club)) =
            extract_teams_from_og_url(&document)?;

        // Extract match ID from URL
        let re = regex::Regex::new(r"/matches/team/(\d+)/(\d+)/(\d+)/(\d+)/(\d+)")?;
        let match_id = re
            .captures(url)
            .ok_or_else(|| anyhow!("Failed to extract match ID from URL"))?
            .get(0)
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| anyhow!("Failed to extract match ID"))?;

        Ok(GameData {
            event_start_time: tx_time, // For now, use tx_time as event_start_time
            original_start_time: None, // We don't have this information yet
            tx_time,
            match_id,
            set_number: 1, // Default values for now
            leg_number: 1,
            competition_type: match_type.to_string(),
            season: "2025".to_string(), // We know this from the URL filtering
            division: "Unknown".to_string(), // Would need to parse this from the page
            venue: "Unknown".to_string(), // Would need to parse this from the page
            home_team_name,
            home_team_club,
            away_team_name,
            away_team_club,
            home_player1: "Unknown".to_string(), // Would need to parse these from the page
            home_player2: None,
            away_player1: "Unknown".to_string(),
            away_player2: None,
            home_score: 0, // Would need to parse these from the page
            away_score: 0,
            handicap_home: 0,
            handicap_away: 0,
            report_html: Some(html.to_string()),
        })
    }

    fn gather_urls(&mut self) -> Result<()> {
        let mut all_new_urls = HashSet::new();
        fs::create_dir_all(HTML_FILES_DIR)?;
        fs::create_dir_all(CACHE_DIR)?;

        // First query to get current total records
        let initial_response = self.query_event_viewer(0, 1)?;
        let current_total_records = initial_response
            .get("recordsTotal")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32)
            .ok_or_else(|| anyhow!("Failed to get total records count"))?;

        info!(
            "Initial query shows total records: {}",
            current_total_records
        );

        // First collect all existing cache files and their ranges
        let mut cached_ranges = Vec::new();
        let mut highest_cached_end = 0;
        if let Ok(entries) = fs::read_dir(CACHE_DIR) {
            for entry in entries.filter_map(Result::ok) {
                if let Some(filename) = entry.file_name().to_str() {
                    if let Some(captures) =
                        regex::Regex::new(r"records_(\d+)_to_(\d+)\.json")?.captures(filename)
                    {
                        if let (Some(start), Some(end)) = (captures.get(1), captures.get(2)) {
                            if let (Ok(start_record), Ok(end_record)) =
                                (start.as_str().parse::<u32>(), end.as_str().parse::<u32>())
                            {
                                // Verify the cache file has valid content
                                let cache_path = entry.path();
                                if let Ok(content) = fs::read_to_string(&cache_path) {
                                    if let Ok(json) =
                                        serde_json::from_str::<serde_json::Value>(&content)
                                    {
                                        if let Some(data) =
                                            json.get("data").and_then(|d| d.as_array())
                                        {
                                            if !data.is_empty() {
                                                cached_ranges.push((start_record, end_record));
                                                highest_cached_end =
                                                    highest_cached_end.max(end_record);
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

        // If we have cached records and there are new records, fetch only the new ones
        let mut ranges_to_fetch = Vec::new();
        if highest_cached_end < current_total_records {
            ranges_to_fetch.push((highest_cached_end, current_total_records));
        }

        if ranges_to_fetch.is_empty() {
            info!("No new event logs to process");
            return Ok(());
        }

        let total_records_to_fetch: u32 =
            ranges_to_fetch.iter().map(|(start, end)| end - start).sum();
        info!(
            "Found {} ranges to fetch, total records: {}",
            ranges_to_fetch.len(),
            total_records_to_fetch
        );
        let total_pages = ((total_records_to_fetch as f64) / (PAGE_SIZE as f64)).ceil() as u64;

        let progress = ProgressBar::new(total_pages);
        progress.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} pages processed ({eta})",
                )
                .unwrap(),
        );
        progress.enable_steady_tick(Duration::from_millis(100));

        // Process each range in PAGE_SIZE chunks
        for (range_start, range_end) in ranges_to_fetch {
            let mut start = range_start;
            while start < range_end {
                let length = PAGE_SIZE.min(range_end - start);

                let response = self.query_event_viewer(start, length)?;
                let new_urls = self.process_api_response(response)?;
                all_new_urls.extend(new_urls);

                start += length;
                progress.inc(1);
            }
        }

        progress.finish_and_clear();

        let total_urls = all_new_urls.len();
        if total_urls > 0 {
            info!("Processing {} URLs from new event logs", total_urls);
            let urls_vec: Vec<(String, DateTime<Utc>)> = all_new_urls.into_iter().collect();
            fs::write(
                URLS_FILE,
                urls_vec
                    .iter()
                    .map(|(url, _)| url)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("\n"),
            )?;
            self.process_urls(urls_vec)?;
        } else {
            info!("No new URLs found in event logs");
        }

        Ok(())
    }

    fn process_urls(&mut self, urls: Vec<(String, DateTime<Utc>)>) -> Result<()> {
        let queue: Arc<Mutex<VecDeque<(String, DateTime<Utc>)>>> =
            Arc::new(Mutex::new(VecDeque::from(urls)));
        let progress = ProgressBar::new(queue.lock().unwrap().len() as u64);
        progress.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} URLs processed ({eta})",
                )
                .unwrap(),
        );

        let mut handles = Vec::new();
        let retry_config = RetryConfig::default();

        for _ in 0..CONCURRENT_REQUESTS {
            let queue = Arc::clone(&queue);
            let client = self.client.clone();
            let stats = Arc::clone(&self.stats);
            let progress = progress.clone();
            let retry_config = retry_config.clone();

            let handle = thread::spawn(move || -> Result<()> {
                let scraper = MatchHtmlScraper::new(ScraperConfig::default())?;
                while let Some((url, tx_time)) = {
                    let mut queue = queue.lock().unwrap();
                    queue.pop_front()
                } {
                    let filepath = Self::url_to_filepath(&url);
                    let result = scraper.retry_with_backoff(
                        || {
                            let response = client.get(&url).send()?;
                            let html = response.text()?;
                            if Self::validate_html(&html) {
                                fs::create_dir_all(HTML_FILES_DIR)?;
                                // Extract and store game data
                                let game_data = Self::extract_game_data(&html, &url, tx_time)?;
                                // Store both HTML and metadata
                                fs::write(&filepath, html)?;
                                let metadata_path = filepath.with_extension("json");
                                fs::write(
                                    &metadata_path,
                                    serde_json::to_string_pretty(&game_data)?,
                                )?;
                                Ok(())
                            } else {
                                Err(anyhow!("Invalid HTML content"))
                            }
                        },
                        retry_config.clone(),
                    );

                    match result {
                        Ok(_) => {
                            stats.lock().unwrap().total_matches_processed += 1;
                        }
                        Err(e) => {
                            error!("Failed to process {}: {}", url, e);
                            stats
                                .lock()
                                .unwrap()
                                .errors
                                .push(format!("Failed to process {}: {}", url, e));
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
        let result_urls: Vec<String> = result.iter().map(|(url, _)| url.clone()).collect();
        info!("Found {} URLs: {:?}", result_urls.len(), result_urls);

        // Check that we got all expected URLs
        assert_eq!(
            result_urls.len(),
            expected_urls.len(),
            "Wrong number of URLs extracted"
        );

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

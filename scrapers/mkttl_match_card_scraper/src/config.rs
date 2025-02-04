use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuestDbConfig {
    pub host: String,
    pub port: u16,
}

impl Default for QuestDbConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 9000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RateLimits {
    pub requests_per_second: u32,
}

impl Default for RateLimits {
    fn default() -> Self {
        Self {
            requests_per_second: 2,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScrapingConfig {
    pub user_agent: String,
    pub request_timeout_secs: u64,
}

impl Default for ScrapingConfig {
    fn default() -> Self {
        Self {
            user_agent: "Mozilla/5.0 (compatible; TTRankings/1.0)".to_string(),
            request_timeout_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScraperConfig {
    pub questdb: QuestDbConfig,
    pub rate_limits: RateLimits,
    pub scraping: ScrapingConfig,
}

impl ScraperConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(host) = env::var("QUESTDB_HOST") {
            config.questdb.host = host;
        }
        if let Ok(port) = env::var("QUESTDB_PORT").map_or(Ok(None), |p| p.parse::<u16>().map(Some)) {
            if let Some(port) = port {
                config.questdb.port = port;
            }
        }
        if let Ok(rps) = env::var("RATE_LIMIT_RPS").map_or(Ok(None), |r| r.parse::<u32>().map(Some)) {
            if let Some(rps) = rps {
                config.rate_limits.requests_per_second = rps;
            }
        }
        if let Ok(user_agent) = env::var("SCRAPER_USER_AGENT") {
            config.scraping.user_agent = user_agent;
        }
        if let Ok(timeout) = env::var("SCRAPER_TIMEOUT_SECS").map_or(Ok(None), |t| t.parse::<u64>().map(Some)) {
            if let Some(timeout) = timeout {
                config.scraping.request_timeout_secs = timeout;
            }
        }

        config
    }
}

impl Default for ScraperConfig {
    fn default() -> Self {
        Self {
            questdb: QuestDbConfig::default(),
            rate_limits: RateLimits::default(),
            scraping: ScrapingConfig::default(),
        }
    }
} 
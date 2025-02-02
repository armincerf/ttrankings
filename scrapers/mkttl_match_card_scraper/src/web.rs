use axum::{
    extract::State,
    response::{Html, IntoResponse, Json},
    routing::get,
    Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use crate::metrics::ScraperMetrics;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScraperStats {
    pub requests_per_second: f64,
    pub total_matches_found: u64,
    pub latest_update: Option<DateTime<Utc>>,
    pub current_page: u32,
    pub total_pages: u32,
    pub total_results: u32,
    pub status: String,
    pub current_url: Option<String>,
    pub metrics: Option<ScraperMetrics>,
}

impl Default for ScraperStats {
    fn default() -> Self {
        Self {
            requests_per_second: 0.0,
            total_matches_found: 0,
            latest_update: None,
            current_page: 0,
            total_pages: 0,
            total_results: 0,
            status: "Initializing".to_string(),
            current_url: None,
            metrics: None,
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub stats: Arc<Mutex<ScraperStats>>,
    pub shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

#[axum::debug_handler]
pub async fn index_handler(State(state): State<AppState>) -> Html<String> {
    let stats = state.stats.lock().unwrap();
    Html(format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>MKTTL Scraper Status</title>
    <script src="https://unpkg.com/@tailwindcss/browser@4"></script>
    <script>
        function updateStats() {{
            fetch('/stats')
                .then(response => response.json())
                .then(stats => {{
                    document.getElementById('rps').textContent = stats.requests_per_second.toFixed(2);
                    document.getElementById('total').textContent = stats.total_matches_found;
                    document.getElementById('page').textContent = `${{stats.current_page}}/${{stats.total_pages}}`;
                    document.getElementById('total_results').textContent = stats.total_results;
                    document.getElementById('status').textContent = stats.status;
                    document.getElementById('progress').style.width = `${{(stats.current_page / stats.total_pages * 100) || 0}}%`;
                    
                    // Update current URL if present
                    const currentUrlEl = document.getElementById('current_url');
                    if (stats.current_url) {{
                        currentUrlEl.textContent = stats.current_url;
                        currentUrlEl.parentElement.classList.remove('hidden');
                    }} else {{
                        currentUrlEl.parentElement.classList.add('hidden');
                    }}

                    // Add status to history only if it's different from the last one
                    const statusHistory = document.getElementById('status_history');
                    const now = new Date().toLocaleTimeString();
                    const newStatusText = `[${{now}}] ${{stats.status}}`;
                    
                    // Only add if it's different from the last status
                    const lastStatus = statusHistory.lastChild?.textContent;
                    if (!lastStatus || !lastStatus.endsWith(stats.status)) {{
                        const newStatus = document.createElement('div');
                        newStatus.className = 'text-sm text-gray-600 mb-1';
                        newStatus.textContent = newStatusText;
                        
                        // Keep only last 10 status messages
                        while (statusHistory.children.length >= 10) {{
                            statusHistory.removeChild(statusHistory.firstChild);
                        }}
                        statusHistory.appendChild(newStatus);
                        statusHistory.scrollTop = statusHistory.scrollHeight;
                    }}

                    if (stats.metrics) {{
                        document.getElementById('metrics').innerHTML = `
                            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                                <div class="bg-white p-4 rounded-lg shadow">
                                    <h3 class="text-lg font-semibold mb-2">Request Stats</h3>
                                    <p>Total requests: ${{stats.metrics.total_requests}}</p>
                                    <p>Successful: ${{stats.metrics.successful_requests}}</p>
                                    <p>Failed: ${{stats.metrics.failed_requests}}</p>
                                </div>
                                <div class="bg-white p-4 rounded-lg shadow">
                                    <h3 class="text-lg font-semibold mb-2">Performance</h3>
                                    <p>Avg response time: ${{stats.metrics.avg_response_time_ms.toFixed(2)}}ms</p>
                                    <p>Rate limiter wait: ${{stats.metrics.rate_limiter_wait_time_ms.toFixed(2)}}ms</p>
                                </div>
                                ${{stats.metrics.last_error ? `
                                <div class="bg-red-50 p-4 rounded-lg shadow">
                                    <h3 class="text-lg font-semibold mb-2 text-red-700">Last Error</h3>
                                    <p class="text-red-600">${{stats.metrics.last_error}}</p>
                                </div>` : ''}}
                            </div>
                        `;
                    }}
                }});
        }}
        // Update more frequently (every 200ms)
        setInterval(updateStats, 200);
        updateStats();
    </script>
</head>
<body class="bg-gray-100 min-h-screen p-8">
    <div class="max-w-6xl mx-auto">
        <h1 class="text-3xl font-bold mb-8 text-gray-800">MKTTL Scraper Status</h1>
        
        <div class="bg-white rounded-lg shadow-lg p-6 mb-8">
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                <div class="bg-blue-50 p-4 rounded-lg">
                    <h2 class="text-sm font-semibold text-blue-600 mb-1">Requests/Second</h2>
                    <p class="text-2xl font-bold text-blue-700" id="rps">{:.2}</p>
                </div>
                <div class="bg-green-50 p-4 rounded-lg">
                    <h2 class="text-sm font-semibold text-green-600 mb-1">Matches Found</h2>
                    <p class="text-2xl font-bold text-green-700" id="total">{}</p>
                </div>
                <div class="bg-purple-50 p-4 rounded-lg">
                    <h2 class="text-sm font-semibold text-purple-600 mb-1">Total Results</h2>
                    <p class="text-2xl font-bold text-purple-700" id="total_results">{}</p>
                </div>
                <div class="bg-yellow-50 p-4 rounded-lg">
                    <h2 class="text-sm font-semibold text-yellow-600 mb-1">Current Page</h2>
                    <p class="text-2xl font-bold text-yellow-700" id="page">{}/{}</p>
                </div>
            </div>
            
            <div class="mt-6">
                <h2 class="text-sm font-semibold text-gray-600 mb-2">Progress</h2>
                <div class="w-full bg-gray-200 rounded-full h-4">
                    <div id="progress" class="bg-blue-600 h-4 rounded-full transition-all duration-500" style="width: {}%"></div>
                </div>
            </div>

            <div class="mt-6">
                <h2 class="text-sm font-semibold text-gray-600 mb-2">Current Status</h2>
                <p class="text-gray-700" id="status">{}</p>
            </div>

            <div class="mt-4 hidden">
                <h2 class="text-sm font-semibold text-gray-600 mb-2">Current URL</h2>
                <p class="text-gray-700 font-mono text-sm break-all" id="current_url"></p>
            </div>

            <div class="mt-6">
                <h2 class="text-sm font-semibold text-gray-600 mb-2">Status History</h2>
                <div id="status_history" class="bg-gray-50 p-4 rounded-lg max-h-40 overflow-y-auto"></div>
            </div>
        </div>

        <div id="metrics" class="mt-8">
            {}
        </div>
    </div>
</body>
</html>"#,
        stats.requests_per_second,
        stats.total_matches_found,
        stats.total_results,
        stats.current_page,
        stats.total_pages,
        (stats.current_page as f64 / stats.total_pages as f64 * 100.0),
        stats.status,
        stats.metrics.as_ref().map_or_else(String::new, |m| format!(
            r#"<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                <div class="bg-white p-4 rounded-lg shadow">
                    <h3 class="text-lg font-semibold mb-2">Request Stats</h3>
                    <p>Total requests: {}</p>
                    <p>Successful: {}</p>
                    <p>Failed: {}</p>
                </div>
                <div class="bg-white p-4 rounded-lg shadow">
                    <h3 class="text-lg font-semibold mb-2">Performance</h3>
                    <p>Avg response time: {:.2}ms</p>
                    <p>Rate limiter wait: {:.2}ms</p>
                </div>
                {}</div>"#,
            m.total_requests,
            m.successful_requests,
            m.failed_requests,
            m.avg_response_time_ms,
            m.rate_limiter_wait_time_ms,
            m.last_error.as_ref().map_or_else(String::new, |e| format!(
                r#"<div class="bg-red-50 p-4 rounded-lg shadow">
                    <h3 class="text-lg font-semibold mb-2 text-red-700">Last Error</h3>
                    <p class="text-red-600">{}</p>
                </div>"#,
                e
            ))
        ))
    ))
}

#[axum::debug_handler]
pub async fn stats_handler(State(state): State<AppState>) -> impl IntoResponse {
    let stats = state.stats.lock().unwrap().clone();
    Json(stats)
}

#[axum::debug_handler]
pub async fn shutdown_handler(State(state): State<AppState>) {
    if let Some(tx) = state.shutdown_tx.lock().unwrap().take() {
        let _ = tx.send(());
    }
}

pub async fn serve(state: AppState) -> oneshot::Receiver<()> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    *state.shutdown_tx.lock().unwrap() = Some(shutdown_tx);

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/stats", get(stats_handler))
        .route("/shutdown", get(shutdown_handler))
        .with_state(state);

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("Web interface available at http://{}", addr);
    
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    shutdown_rx
} 
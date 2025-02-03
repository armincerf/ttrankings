fn gather_urls(&self) -> Result<Vec<String>> {
    info!("Gathering match URLs from teams page");
    let mut stats = self.stats.lock().unwrap();
    stats.status = "Querying teams page for match URLs...".to_string();

    // Create html_files directory if it doesn't exist
    fs::create_dir_all(HTML_FILES_DIR)?;

    // Query the teams page
    let query_url = "https://www.mkttl.co.uk/fixtures-results/teams";
    let response = self.client
        .get(query_url)
        .send()?;

    if !response.status().is_success() {
        anyhow::bail!("Failed to query teams page: HTTP {}", response.status());
    }

    let text = response.text()?;
    info!("Got response from teams page ({} bytes)", text.len());
    
    // Save the HTML response for inspection
    fs::write("fixtures_page.html", &text)?;
    info!("Saved HTML response to fixtures_page.html");

    // Extract match URLs from the HTML
    let urls: Vec<String> = text
        .lines()
        .filter(|line| line.contains("href=\"/matches/team/"))
        .filter_map(|line| {
            if let Some(start) = line.find("href=\"/matches/team/") {
                if let Some(end) = line[start..].find("\"") {
                    let url = &line[start + 6..start + end];
                    // Remove the leading /matches/team/ to get just the ID
                    return Some(url[13..].to_string());
                }
            }
            None
        })
        .collect();

    info!("Found {} match URLs", urls.len());
    stats.total_matches_to_process = urls.len();
    stats.status = format!("Found {} URLs", urls.len());

    // Write URLs to file
    fs::write(URLS_FILE, urls.join("\n"))?;
    info!("Wrote URLs to {}", URLS_FILE);

    Ok(urls)
} 
# MKTTL Match Card Scraper

A Rust application that scrapes and processes match card data from the Milton Keynes Table Tennis League website (mkttl.co.uk). It monitors the event log for match updates, downloads match HTML files, and converts them to CSV format for analysis.

## Features

- Monitors the MKTTL event log for match updates
- Downloads and caches match HTML files
- Converts match data to CSV format with detailed game information
- Supports both batch and individual file processing
- Maintains state to avoid unnecessary downloads
- Validates cache coverage and detects missing/orphaned files
- Rate-limited concurrent downloads
- Progress tracking and detailed logging

## Prerequisites

- Rust (latest stable version)
- Internet access to reach mkttl.co.uk

## Installation

1. Clone the repository
2. Navigate to the scraper directory
3. Build the project:
   ```bash
   cargo build --release
   ```

## Usage

### Scraping HTML Files

To download match HTML files from the website:
```bash
cargo run --release --bin mkttl_match_card_scraper -- scrape-html
```

Optional flags:
- `--limit <number>`: Limit the number of matches to scrape

### Converting HTML to CSV

There are two ways to convert HTML files to CSV:

1. Batch Processing (Recommended):
```bash
cargo run --bin batch_process
```
This will process all HTML files in parallel and show a progress bar.

2. Individual File Processing:
```bash
cargo run --bin mkttl_match_card_scraper -- process-file --file path/to/file.html
```

### Output Format

CSV files are created in the `parsed_html_output` directory with the following columns:
- event_start_time
- match_id
- set_number
- leg_number
- competition_type
- season
- division
- venue
- home_team_name
- home_team_club
- away_team_name
- away_team_club
- home_player1
- home_player2
- away_player1
- away_player2
- home_score
- away_score
- handicap_home
- handicap_away
- report_html

## Project Structure

- `src/match_html_scraper.rs`: Main scraper logic for downloading HTML files
- `src/game_scraper.rs`: HTML to CSV conversion logic
- `src/bin/batch_process.rs`: Parallel batch processing of HTML files
- `html_files/`: Downloaded HTML files
- `html_files/cache/`: Cache of event log data
- `parsed_html_output/`: Generated CSV files

## Development

Run tests:
```bash
cargo test
```

Format code:
```bash
cargo fmt
```

Run linter:
```bash
cargo clippy
``` 
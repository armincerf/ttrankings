# MKTTL Match Card Scraper

This is a Rust application that scrapes match card data from the Milton Keynes Table Tennis League website (mkttl.co.uk). It monitors the event log for match updates and stores the data in QuestDB for further analysis.

## Features

- Monitors the MKTTL event log for match updates
- Extracts match card URLs from event descriptions
- Stores match data in QuestDB with timestamps
- Supports both full and incremental scraping modes
- Maintains a marker file to track the last processed event
- Deduplicates match URLs to avoid duplicate entries

## Prerequisites

- Rust 1.40 or later
- QuestDB server running locally or accessible via network
- Internet access to reach mkttl.co.uk

## Installation

1. Clone the repository
2. Navigate to the scraper directory
3. Build the project:
   ```bash
   cargo build --release
   ```

## Configuration

The scraper uses the following environment variables:

- `QDB_CLIENT_CONF`: QuestDB connection configuration string (default: "http::addr=localhost:9000;")

Example configuration:
```bash
export QDB_CLIENT_CONF="http::addr=localhost:9000;username=admin;password=quest;"
```

## Usage

Run the scraper:
```bash
cargo run --release
```

The first run will perform a full scrape of all available match data. Subsequent runs will only fetch new updates since the last run.

## Data Storage

Match data is stored in QuestDB in the `mkttl_matches` table with the following schema:

- `timestamp` (TIMESTAMP): When the match was updated
- `url` (STRING): The match card URL (without protocol)
- `updated_by` (SYMBOL): Username of the person who updated the match
- `tx_time` (TIMESTAMP): Time of insertion into QuestDB

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
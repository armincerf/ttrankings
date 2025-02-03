# Table Tennis Analysis Module

This module provides tools for analyzing table tennis match data from QuestDB. It's designed to be efficient and easy to use, with support for parallel query execution and formatted output.

## Quick Start

```rust
use mkttl_match_card_scraper::analysis::Analysis;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let analysis = Analysis::new("localhost:9000".to_string());
    
    // Analyze a single player
    let stats = analysis.analyze_player("Alex Davis").await?;
    println!("{}", stats.summary());
    
    // Run custom queries in parallel
    let queries = vec![
        ("matches", "SELECT COUNT(*) FROM table_tennis_games"),
        ("players", "SELECT DISTINCT home_player1 FROM table_tennis_games"),
    ];
    let results = analysis.execute_queries(&queries).await?;
    
    Ok(())
}
```

## Key Features

1. **Parallel Query Execution**: Run multiple queries simultaneously for faster analysis
2. **Type-Safe Results**: Query results are properly typed and easy to work with
3. **Built-in Analysis**: Common analyses like player statistics are pre-built
4. **Formatted Output**: Results can be easily formatted for display

## Common Analysis Tasks

### Player Analysis
```rust
let stats = analysis.analyze_player("Player Name").await?;
println!("{}", stats.summary());
```

This will show:
- Total matches and games played
- Overall win rate
- Breakdown by season
- Win rates per season

### Custom Queries
```rust
let result = analysis.execute_query("YOUR QUERY HERE").await?;
```

### Multiple Queries
```rust
let queries = vec![
    ("name1", "QUERY1"),
    ("name2", "QUERY2"),
];
let results = analysis.execute_queries(&queries).await?;
```

## Tips for Writing Queries

1. Use `DISTINCT` when counting unique items
2. Group results by relevant dimensions (season, competition_type, etc.)
3. Use meaningful aliases in SELECT statements
4. Consider using CTEs for complex queries

## Example Queries

### Player Performance
```sql
SELECT 
    season,
    COUNT(*) as games_played,
    SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) as games_won
FROM table_tennis_games
WHERE home_player1 = 'Player Name'
GROUP BY season
```

### Match Analysis
```sql
SELECT 
    competition_type,
    COUNT(DISTINCT match_id) as matches,
    AVG(home_score) as avg_home_score
FROM table_tennis_games
GROUP BY competition_type
```

## Future Improvements

1. Add more pre-built analyses
2. Support for team statistics
3. Head-to-head comparisons
4. Tournament/Cup specific analysis
5. Time-based trends and patterns 
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::error::Error;

#[derive(Debug, Serialize, Deserialize)]
pub struct Match {
    pub week_beginning: String,
    pub competition: String,
    pub date: String,
    pub home_team: String,
    pub away_team: String,
    pub score: String,
    pub venue: String,
    pub scorecard_url: Option<String>,
}

pub fn scrape_fixtures(html: &str) -> Result<Vec<Match>, Box<dyn Error>> {
    let document = Html::parse_document(html);
    let table_selector = Selector::parse("table#fixtures-table").unwrap();
    let row_selector = Selector::parse("tr").unwrap();
    let cell_selector = Selector::parse("td").unwrap();
    let link_selector = Selector::parse("a").unwrap();

    let mut matches = Vec::new();

    if let Some(table) = document.select(&table_selector).next() {
        for row in table.select(&row_selector).skip(1) { // Skip header row
            let cells: Vec<_> = row.select(&cell_selector).collect();
            
            if cells.len() >= 14 {
                let week_beginning = cells[1].text().collect::<String>().trim().to_string();
                let competition = cells[3].text().collect::<String>().trim().to_string();
                let date = cells[7].text().collect::<String>().trim().to_string();
                let home_team = cells[8].text().collect::<String>().trim().to_string();
                let away_team = cells[10].text().collect::<String>().trim().to_string();
                
                // Extract score and scorecard URL if available
                let score_cell = &cells[12];
                let score = score_cell.text().collect::<String>().trim().to_string();
                let scorecard_url = score_cell
                    .select(&link_selector)
                    .next()
                    .and_then(|a| a.value().attr("href"))
                    .map(|href| format!("https://www.mkttl.co.uk{}", href));

                let venue = cells[13].text().collect::<String>().trim().to_string();

                matches.push(Match {
                    week_beginning,
                    competition,
                    date,
                    home_team,
                    away_team,
                    score,
                    venue,
                    scorecard_url,
                });
            }
        }
    }

    Ok(matches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_scrape_fixtures() {
        let html = fs::read_to_string("tests/fixtures/fixutre_scraper/fixtures-results_months_2025_02.html").unwrap();
        let matches = scrape_fixtures(&html).unwrap();
        
        assert!(!matches.is_empty());
        
        // Test first match
        let first_match = &matches[0];
        assert_eq!(first_match.competition, "League - Premier Division");
        assert_eq!(first_match.score, "9-1");
        assert!(first_match.scorecard_url.is_some());
        assert_eq!(first_match.home_team, "Greenleys Warriors");
        assert_eq!(first_match.away_team, "Milton Keynes Topspin");
        assert_eq!(first_match.venue, "Milton Keynes Table Tennis Centre");
    }
} 
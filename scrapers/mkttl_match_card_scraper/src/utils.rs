use anyhow::{Context, Result};
use scraper::{Html, Selector};
use crate::types::MatchType;

pub fn detect_match_type(document: &Html) -> Result<MatchType> {
    // First try the title tag
    let title_selector = Selector::parse("title").unwrap();
    let mut title = document
        .select(&title_selector)
        .next()
        .map(|el| el.text().collect::<String>());

    // If title tag is not found, try og:title meta tag
    if title.is_none() {
        let og_title_selector = Selector::parse("meta[property='og:title']").unwrap();
        title = document
            .select(&og_title_selector)
            .next()
            .and_then(|el| el.value().attr("content"))
            .map(|s| s.to_string());
    }

    // Check the h2 tag
    let h2_selector = Selector::parse("h2").unwrap();
    let h2_text = document
        .select(&h2_selector)
        .next()
        .map(|el| el.text().collect::<String>());

    // If either the title or h2 contains "Cup", it's a cup match
    if title.as_ref().map(|t| t.contains("Cup")).unwrap_or(false) 
        || h2_text.as_ref().map(|t| t.contains("Cup")).unwrap_or(false) {
        Ok(MatchType::MkttlChallengeCup)
    } else if title.is_some() || h2_text.is_some() {
        Ok(MatchType::MkttlLeagueMatch)
    } else {
        anyhow::bail!("No title, og:title, or h2 found")
    }
}

pub fn parse_score(score: &str) -> Result<(i32, i32)> {
    let parts: Vec<&str> = score.split('-').collect();
    if parts.len() != 2 {
        anyhow::bail!("Invalid score format: {}", score);
    }

    let home_score = parts[0]
        .trim()
        .parse::<i32>()
        .with_context(|| format!("Invalid home score: {}", parts[0]))?;
    let away_score = parts[1]
        .trim()
        .parse::<i32>()
        .with_context(|| format!("Invalid away score: {}", parts[1]))?;

    Ok((home_score, away_score))
}

fn capitalize_words(s: &str) -> String {
    s.split_whitespace()
        .map(|word| {
            let mut chars = word.chars().collect::<Vec<_>>();
            if let Some(first) = chars.first_mut() {
                *first = first.to_uppercase().next().unwrap_or(*first);
            }
            for c in chars.iter_mut().skip(1) {
                *c = c.to_lowercase().next().unwrap_or(*c);
            }
            chars.into_iter().collect::<String>()
        })
        .collect::<Vec<String>>()
        .join(" ")
}

pub fn extract_teams_from_og_url(document: &Html) -> Result<((String, String), (String, String))> {
    let og_url_selector = Selector::parse("meta[property='og:url']").unwrap();
    let og_url = document
        .select(&og_url_selector)
        .next()
        .ok_or_else(|| anyhow::anyhow!("No og:url meta tag found"))?
        .value()
        .attr("content")
        .ok_or_else(|| anyhow::anyhow!("No content attribute in og:url meta tag"))?;

    // URL format: https://www.mkttl.co.uk/matches/team/club1/team1/club2/team2/year/month/day
    let parts: Vec<&str> = og_url.split('/').collect();
    if parts.len() < 9 {
        anyhow::bail!("Invalid URL format: {}", og_url);
    }

    // Extract team and club names from the URL parts
    let home_team_club = parts[parts.len() - 7].replace("-", " ");
    let home_team_name = parts[parts.len() - 6].replace("-", " ");
    let away_team_club = parts[parts.len() - 5].replace("-", " ");
    let away_team_name = parts[parts.len() - 4].replace("-", " ");

    Ok((
        (capitalize_words(&home_team_name), capitalize_words(&home_team_club)),
        (capitalize_words(&away_team_name), capitalize_words(&away_team_club)),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use scraper::Html;

    #[test]
    fn test_parse_score() {
        // Valid scores
        assert_eq!(parse_score("11-9").unwrap(), (11, 9));
        assert_eq!(parse_score("9-11").unwrap(), (9, 11));
        assert_eq!(parse_score(" 11 - 9 ").unwrap(), (11, 9));
        assert_eq!(parse_score("0-0").unwrap(), (0, 0));
        
        // Invalid scores
        assert!(parse_score("invalid").is_err());
        assert!(parse_score("11:9").is_err());
        assert!(parse_score("11").is_err());
        assert!(parse_score("-11-9").is_err());
        assert!(parse_score("abc-def").is_err());
    }

    #[test]
    fn test_capitalize_words() {
        assert_eq!(capitalize_words("hello world"), "Hello World");
        assert_eq!(capitalize_words("HELLO WORLD"), "Hello World");
        assert_eq!(capitalize_words("hello"), "Hello");
        assert_eq!(capitalize_words("h"), "H");
        assert_eq!(capitalize_words(""), "");
        assert_eq!(capitalize_words("hello   world"), "Hello World");
        assert_eq!(capitalize_words("hello-world"), "Hello-world");
        assert_eq!(capitalize_words("   hello   world   "), "Hello World");
    }

    #[test]
    fn test_detect_match_type() {
        // Test cup match detection
        let cup_html = Html::parse_document(r#"
            <html>
                <head>
                    <title>MKTTL Challenge Cup Match</title>
                </head>
                <body>
                    <h2>Some Cup Match Details</h2>
                </body>
            </html>
        "#);
        assert_eq!(detect_match_type(&cup_html).unwrap(), MatchType::MkttlChallengeCup);

        // Test league match detection
        let league_html = Html::parse_document(r#"
            <html>
                <head>
                    <title>MKTTL League Match</title>
                </head>
                <body>
                    <h2>League Match Details</h2>
                </body>
            </html>
        "#);
        assert_eq!(detect_match_type(&league_html).unwrap(), MatchType::MkttlLeagueMatch);

        // Test with og:title
        let og_title_html = Html::parse_document(r#"
            <html>
                <head>
                    <meta property="og:title" content="MKTTL Challenge Cup Match" />
                </head>
            </html>
        "#);
        assert_eq!(detect_match_type(&og_title_html).unwrap(), MatchType::MkttlChallengeCup);

        // Test with no title but cup in h2
        let h2_cup_html = Html::parse_document(r#"
            <html>
                <body>
                    <h2>Cup Match Details</h2>
                </body>
            </html>
        "#);
        assert_eq!(detect_match_type(&h2_cup_html).unwrap(), MatchType::MkttlChallengeCup);
    }

    #[test]
    fn test_extract_teams_from_og_url() {
        let html = Html::parse_document(r#"
            <html>
                <head>
                    <meta property="og:url" content="https://www.mkttl.co.uk/matches/team/milton-keynes/team-a/newport-pagnell/team-b/2024/01/31" />
                </head>
            </html>
        "#);
        
        let ((home_team, home_club), (away_team, away_club)) = extract_teams_from_og_url(&html).unwrap();
        assert_eq!(home_team, "Team A");
        assert_eq!(home_club, "Milton Keynes");
        assert_eq!(away_team, "Team B");
        assert_eq!(away_club, "Newport Pagnell");

        // Test invalid URL
        let invalid_html = Html::parse_document(r#"
            <html>
                <head>
                    <meta property="og:url" content="https://www.mkttl.co.uk/invalid/url" />
                </head>
            </html>
        "#);
        assert!(extract_teams_from_og_url(&invalid_html).is_err());

        // Test missing og:url
        let missing_og_url_html = Html::parse_document("<html></html>");
        assert!(extract_teams_from_og_url(&missing_og_url_html).is_err());
    }
} 
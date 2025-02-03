use anyhow::{Context, Result};
use scraper::{Html, Selector};
use crate::types::{MatchType, KNOWN_CLUBS};

pub fn detect_match_type(document: &Html) -> Result<MatchType> {
    // First try the title tag
    let title_selector = Selector::parse("title").unwrap();
    let title = document
        .select(&title_selector)
        .next()
        .map(|el| el.text().collect::<String>());

    // If title tag is not found, try og:title meta tag
    let title = match title {
        Some(t) => t,
        None => {
            let og_title_selector = Selector::parse("meta[property='og:title']").unwrap();
            document
                .select(&og_title_selector)
                .next()
                .and_then(|el| el.value().attr("content"))
                .ok_or_else(|| anyhow::anyhow!("No title or og:title found"))?
                .to_string()
        }
    };

    // Also check the h2 tag
    let h2_selector = Selector::parse("h2").unwrap();
    let h2_text = document
        .select(&h2_selector)
        .next()
        .map(|el| el.text().collect::<String>());

    // If either the title or h2 contains "Cup", it's a cup match
    if title.contains("Cup") || h2_text.map(|t| t.contains("Cup")).unwrap_or(false) {
        Ok(MatchType::MkttlChallengeCup)
    } else {
        Ok(MatchType::MkttlLeagueMatch)
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

pub fn split_team_name(full_name: &str) -> Result<(String, String)> {
    let words: Vec<&str> = full_name.split_whitespace().collect();
    if words.is_empty() {
        return Err(anyhow::Error::msg("Empty team name"));
    }

    // Try to find a known club name in the full name
    for club in KNOWN_CLUBS {
        if full_name.starts_with(club) {
            // Everything after the club name is the team name
            let team_name = full_name[club.len()..].trim().to_string();
            return Ok((club.to_string(), team_name));
        }
    }

    // If no known club is found, fall back to the old behavior
    let team_name = words.last().unwrap().to_string();
    let club_name = words[..words.len()-1].join(" ");

    Ok((club_name, team_name))
}

fn capitalize_words(s: &str) -> String {
    s.split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().chain(chars).collect(),
            }
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

    #[test]
    fn test_split_team_name() {
        assert_eq!(
            split_team_name("Little Horwood Destroyers").unwrap(),
            ("Little Horwood".to_string(), "Destroyers".to_string())
        );
        assert_eq!(
            split_team_name("Milton Keynes Musketeers").unwrap(),
            ("Milton Keynes".to_string(), "Musketeers".to_string())
        );
        assert_eq!(
            split_team_name("Greenleys Glory").unwrap(),
            ("Greenleys".to_string(), "Glory".to_string())
        );
    }

    #[test]
    fn test_parse_score() {
        assert_eq!(parse_score("11-9").unwrap(), (11, 9));
        assert_eq!(parse_score("9-11").unwrap(), (9, 11));
        assert!(parse_score("invalid").is_err());
    }
} 
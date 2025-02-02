use anyhow::{Context, Result};
use scraper::{Html, Selector};
use crate::types::{MatchType, KNOWN_CLUBS};

pub fn detect_match_type(html: &str) -> Result<MatchType> {
    let document = Html::parse_document(html);
    let competition_type = document
        .select(&Selector::parse("h2").unwrap())
        .next()
        .map(|el| el.text().collect::<String>().trim().to_string())
        .context("Could not find competition type")?;

    match competition_type.as_str() {
        "League" => Ok(MatchType::MkttlLeagueMatch),
        "Challenge Cup" => Ok(MatchType::MkttlChallengeCup),
        _ => Err(anyhow::anyhow!("Unknown match type: {}", competition_type)),
    }
}

pub fn parse_leg_score(score: &str) -> Result<(i32, i32)> {
    let parts: Vec<&str> = score.split("-").collect();
    if parts.len() != 2 {
        anyhow::bail!("Invalid score format");
    }

    Ok((
        parts[0].trim().parse()?,
        parts[1].trim().parse()?,
    ))
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
    fn test_parse_leg_score() {
        assert_eq!(parse_leg_score("11-9").unwrap(), (11, 9));
        assert_eq!(parse_leg_score("9-11").unwrap(), (9, 11));
        assert!(parse_leg_score("invalid").is_err());
    }
} 
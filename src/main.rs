use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dotenv::dotenv;
use reqwest::header::{AUTHORIZATION, USER_AGENT};
use serde::Deserialize;
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{collections::HashSet, env, time::Duration};
use tokio::time::sleep;

#[derive(Debug, Deserialize)]
struct SearchResponse {
    items: Vec<SearchItem>,
}

#[derive(Debug, Deserialize)]
struct SearchItem {
    repository_url: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let token = env::var("GITHUB_TOKEN").expect("GITHUB_TOKEN must be set");
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    println!("🔌 Connecting to Supabase...");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .context("Failed to connect to database. Check your DATABASE_URL.")?;
    println!("✅ Database connected.");

    let client = reqwest::Client::new();
    let languages = vec!["Python", "Go", "Rust", "JavaScript", "TypeScript", "Java", "cpp"];

    for language in languages {
        println!("\n========================================================");
        println!("🚀 LANGUAGE: {}", language.to_uppercase());
        println!("========================================================");

        let mut unique_repos = HashSet::new();
        let mut valid_repos_found = 0;
        let mut page = 1;

        while valid_repos_found < 20 {
            let query = format!(
                "https://api.github.com/search/issues?q=is:issue is:open label:\"good first issue\" language:{}&per_page=30&page={}",
                language, page
            );

            let response = client
                .get(&query)
                .header(USER_AGENT, "rust-github-scraper")
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .send()
                .await?;

            let data: SearchResponse = response.json().await.unwrap_or(SearchResponse { items: vec![] });

            if data.items.is_empty() {
                println!("⚠️ No more results found for {} on page {}.", language, page);
                break;
            }

            for item in data.items {
                if valid_repos_found >= 20 {
                    break;
                }

                let repo_full_name = item.repository_url.split("/repos/").nth(1).unwrap_or("").to_string();

                if unique_repos.contains(&repo_full_name) || repo_full_name.is_empty() {
                    continue;
                }
                unique_repos.insert(repo_full_name.clone());

                let parts: Vec<&str> = repo_full_name.split('/').collect();
                if parts.len() != 2 { continue; }
                let owner = parts[0];
                let name = parts[1];

                println!("\n📦 Processing: {}/{} | 🔗 https://github.com/{}", owner, name, repo_full_name);

                match fetch_graphql_data(&client, &token, owner, name).await {
                    Ok(repo_data) => {
                        match process_and_save_repo(&pool, &repo_data, owner, name, language, &repo_full_name).await {
                            Ok(true) => {
                                valid_repos_found += 1;
                                println!("✅ [{}/20] Saved {} to Supabase", valid_repos_found, language);
                            }
                            Ok(false) => { /* Skipped due to filters */ }
                            Err(e) => println!("❌ Database Error: {}", e),
                        }
                    }
                    Err(e) => println!("⚠️ GraphQL Fetch Failed: {}", e),
                }

                sleep(Duration::from_millis(500)).await; // Polite API delay
            }
            page += 1;
        }
    }

    Ok(())
}

// ================= GRAPHQL ENGINE =================

async fn fetch_graphql_data(
    client: &reqwest::Client,
    token: &str,
    owner: &str,
    name: &str,
) -> Result<serde_json::Value> {
    let since = (Utc::now() - ChronoDuration::days(30)).to_rfc3339();

    let query = r#"
    query($owner: String!, $name: String!, $since: GitTimestamp!) {
      repository(owner: $owner, name: $name) {
        stargazerCount
        forkCount
        pushedAt
        issues(states: OPEN) { totalCount }
        defaultBranchRef {
          target { ... on Commit { history(since: $since) { totalCount } } }
        }
        recentIssues: issues(last: 30, states: OPEN) {
          nodes {
            title
            url
            createdAt
            labels(first: 5) { nodes { name } }
            comments(first: 1) { nodes { createdAt } }
          }
        }
        recentPRs: pullRequests(last: 20, states: MERGED) {
          nodes {
            createdAt
            mergedAt
            additions
            deletions
            changedFiles
            totalCommentsCount
          }
        }
      }
    }
    "#;

    let payload = json!({
        "query": query,
        "variables": { "owner": owner, "name": name, "since": since }
    });

    let res: serde_json::Value = client
        .post("https://api.github.com/graphql")
        .header(USER_AGENT, "rust-github-scraper")
        .header(AUTHORIZATION, format!("Bearer {}", token))
        .json(&payload)
        .send()
        .await?
        .json()
        .await?;

    if let Some(errors) = res.get("errors") {
        anyhow::bail!("GraphQL Error: {}", errors);
    }

    Ok(res["data"]["repository"].clone())
}

// ================= DATA PROCESSING & DB INSERT =================

async fn process_and_save_repo(
    pool: &PgPool,
    repo: &serde_json::Value,
    _owner: &str,
    name: &str,
    language: &str,
    full_name: &str,
) -> Result<bool> {
    if repo.is_null() { return Ok(false); }

    let stars = repo["stargazerCount"].as_i64().unwrap_or(0);
    if stars < 10 {
        println!("   ⏭️ Skipping: Too few stars ({})", stars);
        return Ok(false);
    }

    let forks = repo["forkCount"].as_i64().unwrap_or(0);
    let commits_30d = repo["defaultBranchRef"]["target"]["history"]["totalCount"].as_i64().unwrap_or(0);
    
    let pushed_at_str = repo["pushedAt"].as_str().unwrap_or("");
    let last_active = DateTime::parse_from_rfc3339(pushed_at_str)
        .unwrap_or_else(|_| Utc::now().into())
        .with_timezone(&Utc);

    // --- Calculate Metrics ---
    let mut response_total_hrs = 0.0;
    let mut response_count = 0;

    if let Some(issues) = repo["recentIssues"]["nodes"].as_array() {
        for issue in issues {
            if let (Some(created_str), Some(comments)) = (
                issue["createdAt"].as_str(),
                issue["comments"]["nodes"].as_array(),
            ) {
                if let Some(first_comment) = comments.first() {
                    if let Some(comment_created_str) = first_comment["createdAt"].as_str() {
                        if let (Ok(c), Ok(cm)) = (
                            DateTime::parse_from_rfc3339(created_str),
                            DateTime::parse_from_rfc3339(comment_created_str),
                        ) {
                            response_total_hrs += (cm - c).num_hours() as f64;
                            response_count += 1;
                        }
                    }
                }
            }
        }
    }
    let response_time_avg = if response_count > 0 { response_total_hrs / response_count as f64 } else { 0.0 };

    let mut merge_total_hrs = 0.0;
    let mut total_lines = 0.0;
    let mut total_files = 0.0;
    let mut total_comments = 0.0;
    let mut pr_count = 0.0;

    if let Some(prs) = repo["recentPRs"]["nodes"].as_array() {
        for pr in prs {
            if let (Some(created_str), Some(merged_str)) = (pr["createdAt"].as_str(), pr["mergedAt"].as_str()) {
                if let (Ok(c), Ok(m)) = (
                    DateTime::parse_from_rfc3339(created_str),
                    DateTime::parse_from_rfc3339(merged_str),
                ) {
                    merge_total_hrs += (m - c).num_hours() as f64;
                    total_lines += pr["additions"].as_f64().unwrap_or(0.0) + pr["deletions"].as_f64().unwrap_or(0.0);
                    total_files += pr["changedFiles"].as_f64().unwrap_or(0.0);
                    total_comments += pr["totalCommentsCount"].as_f64().unwrap_or(0.0);
                    pr_count += 1.0;
                }
            }
        }
    }

    let mut pr_merge_avg = 0.0;
    let mut normalized_difficulty: Option<f64> = None;

    if pr_count > 0.0 {
        pr_merge_avg = merge_total_hrs / pr_count;
        let avg_lines = total_lines / pr_count;
        let avg_files = total_files / pr_count;
        let avg_comments = total_comments / pr_count;
        let avg_days = pr_merge_avg / 24.0;

        let scaled_lines = (avg_lines + 1.0).ln();
        let scaled_files = (avg_files + 1.0).ln();
        let scaled_comments = (avg_comments + 1.0).ln();
        let scaled_days = (avg_days + 1.0).ln();

        let raw_score = (scaled_lines * 0.4) + (scaled_files * 0.3) + (scaled_comments * 0.2) + (scaled_days * 0.1);
        normalized_difficulty = Some(raw_score * 10.0);
    }

    let health_score = normalized_difficulty.map(|d| 100.0 - d).unwrap_or(50.0);

    let health_json = json!({
        "commits_30d": commits_30d,
        "response_time_avg": response_time_avg,
        "pr_merge_avg": pr_merge_avg
    });

    // --- 1. UPSERT Repository (NO MACRO BANG!) ---
    sqlx::query(
        r#"
        INSERT INTO repos (full_name, name, stars, forks, health_score, language, last_active, health_json)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (full_name) DO UPDATE 
        SET stars = EXCLUDED.stars, forks = EXCLUDED.forks, health_score = EXCLUDED.health_score, 
            last_active = EXCLUDED.last_active, health_json = EXCLUDED.health_json
        "#
    )
    .bind(full_name)
    .bind(name)
    .bind(stars as i32)
    .bind(forks as i32)
    .bind(health_score)
    .bind(language)
    .bind(last_active)
    .bind(&health_json)
    .execute(pool)
    .await?;

    // --- 2. UPSERT Issues (NO MACRO BANG!) ---
    if let Some(issues) = repo["recentIssues"]["nodes"].as_array() {
        for issue in issues {
            let title = issue["title"].as_str().unwrap_or("Untitled");
            let url = issue["url"].as_str().unwrap_or("");
            if url.is_empty() { continue; }

            let created_str = issue["createdAt"].as_str().unwrap_or("");
            let created_at = DateTime::parse_from_rfc3339(created_str)
                .unwrap_or_else(|_| Utc::now().into())
                .with_timezone(&Utc);

            let mut label_names = Vec::new();
            if let Some(labels_array) = issue["labels"]["nodes"].as_array() {
                for label in labels_array {
                    if let Some(label_name) = label["name"].as_str() {
                        label_names.push(label_name.to_string());
                    }
                }
            }
            let labels_json = json!(label_names);

            sqlx::query(
                r#"
                INSERT INTO issues (url, repo_id, title, difficulty_score, labels, created_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (url) DO UPDATE 
                SET title = EXCLUDED.title, difficulty_score = EXCLUDED.difficulty_score, labels = EXCLUDED.labels
                "#
            )
            .bind(url)
            .bind(full_name)
            .bind(title)
            .bind(normalized_difficulty)
            .bind(&labels_json)
            .bind(created_at)
            .execute(pool)
            .await?;
        }
    }

    Ok(true)
}
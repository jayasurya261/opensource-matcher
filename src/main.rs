use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dotenv::dotenv;
use reqwest::header::{AUTHORIZATION, USER_AGENT};
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashSet, env, time::Duration};
use tokio::time::sleep;

#[derive(Debug, Deserialize)]
struct SearchResponse {
    items: Vec<Issue>,
}

#[derive(Debug, Deserialize)]
struct Issue {
    repository_url: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let token = env::var("GITHUB_TOKEN").expect("GITHUB_TOKEN must be set");
    let client = reqwest::Client::new();

    let languages = vec!["Python", "Rust", "Go"]; // Shortened for testing

    for language in languages {
        println!("\n==============================");
        println!("🚀 LANGUAGE: {}", language);
        println!("==============================");

        // 1. Find Repositories using REST Search (Still the best way to discover repos)
        let query = format!(
            "https://api.github.com/search/issues?q=is:issue is:open label:\"good first issue\" language:{}&per_page=20&page=1",
            language
        );

        let response = client
            .get(&query)
            .header(USER_AGENT, "rust-github-scraper")
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .send()
            .await?;

        let data: SearchResponse = response.json().await?;
        let mut unique_repos = HashSet::new();

        for issue in data.items {
            let repo_full_name = issue
                .repository_url
                .split("/repos/")
                .nth(1)
                .unwrap_or("")
                .to_string();

            if unique_repos.contains(&repo_full_name) || repo_full_name.is_empty() {
                continue;
            }
            unique_repos.insert(repo_full_name.clone());

            let parts: Vec<&str> = repo_full_name.split('/').collect();
            if parts.len() != 2 {
                continue;
            }
            let owner = parts[0];
            let name = parts[1];

            println!("\n📦 Repo: {}/{}", owner, name);

            // 2. Fetch EVERYTHING else in ONE GraphQL call
            match fetch_graphql_data(&client, &token, owner, name).await {
                Ok(repo_data) => process_and_print_repo(repo_data),
                Err(e) => println!("⚠️ Failed to fetch data for {}: {}", repo_full_name, e),
            }

            // Sleep slightly to be polite to the GraphQL endpoint
            sleep(Duration::from_millis(500)).await;
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

    // The Ultimate GraphQL Query: Gets basic info, commits, issues, and PRs in one shot
    let query = r#"
    query($owner: String!, $name: String!, $since: GitTimestamp!) {
      repository(owner: $owner, name: $name) {
        stargazerCount
        forkCount
        pushedAt
        issues(states: OPEN) { totalCount }
        defaultBranchRef {
          target {
            ... on Commit { history(since: $since) { totalCount } }
          }
        }
        recentIssues: issues(last: 30) {
          nodes {
            createdAt
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
        "variables": {
            "owner": owner,
            "name": name,
            "since": since
        }
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

// ================= DATA PROCESSING =================

fn process_and_print_repo(repo: serde_json::Value) {
    if repo.is_null() {
        return;
    }

    // --- 1. Basic Filters & Info ---
    let stars = repo["stargazerCount"].as_u64().unwrap_or(0);
    let forks = repo["forkCount"].as_u64().unwrap_or(0);
    
    // 🔥 FILTER: Skip dead or unloved repos immediately
    if stars < 10 {
        println!("   ⏭️ Skipping: Too few stars ({})", stars);
        return;
    }

    let open_issues = repo["issues"]["totalCount"].as_u64().unwrap_or(0);
    let commits_30d = repo["defaultBranchRef"]["target"]["history"]["totalCount"].as_u64().unwrap_or(0);

    println!("⭐ Stars: {} | 🍴 Forks: {} | 📂 Open Issues: {}", stars, forks, open_issues);
    println!("📊 30d Commits: {}", commits_30d);

    // --- 2. Avg Issue Response Time ---
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
                        if let (Ok(created), Ok(commented)) = (
                            DateTime::parse_from_rfc3339(created_str),
                            DateTime::parse_from_rfc3339(comment_created_str),
                        ) {
                            let duration = commented.with_timezone(&Utc) - created.with_timezone(&Utc);
                            response_total_hrs += duration.num_hours() as f64;
                            response_count += 1;
                        }
                    }
                }
            }
        }
    }
    
    let avg_response = if response_count > 0 { response_total_hrs / response_count as f64 } else { 0.0 };
    println!("⏱ Avg Issue Response (hrs): {:.1}", avg_response);

    // --- 3. PR Merge Time & Normalized Difficulty ---
    let mut merge_total_hrs = 0.0;
    let mut total_lines = 0.0;
    let mut total_files = 0.0;
    let mut total_comments = 0.0;
    let mut pr_count = 0.0;

    if let Some(prs) = repo["recentPRs"]["nodes"].as_array() {
        for pr in prs {
            if let (Some(created_str), Some(merged_str)) = (
                pr["createdAt"].as_str(),
                pr["mergedAt"].as_str(),
            ) {
                if let (Ok(created), Ok(merged)) = (
                    DateTime::parse_from_rfc3339(created_str),
                    DateTime::parse_from_rfc3339(merged_str),
                ) {
                    let duration = merged.with_timezone(&Utc) - created.with_timezone(&Utc);
                    merge_total_hrs += duration.num_hours() as f64;
                    
                    total_lines += pr["additions"].as_f64().unwrap_or(0.0) + pr["deletions"].as_f64().unwrap_or(0.0);
                    total_files += pr["changedFiles"].as_f64().unwrap_or(0.0);
                    total_comments += pr["totalCommentsCount"].as_f64().unwrap_or(0.0);
                    
                    pr_count += 1.0;
                }
            }
        }
    }

    if pr_count > 0.0 {
        let avg_merge = merge_total_hrs / pr_count;
        println!("🔀 Avg PR Merge Time (hrs): {:.1}", avg_merge);

        let avg_lines = total_lines / pr_count;
        let avg_files = total_files / pr_count;
        let avg_comments = total_comments / pr_count;
        let avg_days = avg_merge / 24.0;

        // 🔥 NORMALIZED DIFFICULTY MATH
        let scaled_lines = (avg_lines + 1.0).ln();
        let scaled_files = (avg_files + 1.0).ln();
        let scaled_comments = (avg_comments + 1.0).ln();
        let scaled_days = (avg_days + 1.0).ln();

        let raw_score = (scaled_lines * 0.4) 
                      + (scaled_files * 0.3) 
                      + (scaled_comments * 0.2) 
                      + (scaled_days * 0.1);

        let normalized_score = raw_score * 10.0; // Scales it to roughly 0-100
        
        println!("🧠 Normalized Difficulty Score: {:.2}", normalized_score);
    } else {
        println!("🔀 Avg PR Merge Time: N/A");
        println!("🧠 Normalized Difficulty Score: N/A");
    }
}
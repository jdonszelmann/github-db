use std::{path::Path, sync::Arc};

use rust_query::{
    Database, DatabaseAsync,
    migration::{Config, schema},
};

#[schema(Schema)]
#[version(0..=0)]
pub mod vN {
    pub struct Config {
        #[unique]
        pub key: String,
        pub value: String,
    }

    pub struct Request {
        pub category: i64,
        #[unique]
        pub sequence_number: i64,
        pub data: Vec<u8>,
        pub name: String,
    }

    pub struct User {
        #[unique]
        pub github_id: i64,
        pub name: String,
        pub display_name: String,
    }

    #[unique(organization, name)]
    pub struct Repo {
        pub organization: String,
        pub name: String,
    }

    pub struct IssuePullRequestShared {
        #[unique]
        pub number: i64,

        pub title: String,
        pub description: String,
        pub author: User,

        pub created_timestamp: i64,
        pub updated_timestamp: i64,
        pub closed_at_timestamp: Option<i64>,

        // bool
        pub locked: i64,

        pub repo: Repo,
    }

    #[unique(user, issue_or_pr)]
    pub struct Assignment {
        pub user: User,
        pub issue_or_pr: IssuePullRequestShared,
        pub outdated: i64,
    }

    pub struct PullRequest {
        #[unique]
        pub shared: IssuePullRequestShared,
        // bool
        pub draft: i64,
        // bool
        pub maintainer_can_modify: i64,

        pub num_additions: i64,
        pub num_deletions: i64,
        pub num_changed_files: i64,
        pub num_commits: i64,
    }

    pub struct Issue {
        #[unique]
        pub shared: IssuePullRequestShared,
    }

    #[unique(from, to)]
    pub struct IssuePrLink {
        pub from: IssuePullRequestShared,
        pub to: IssuePullRequestShared,
        pub pr_closes_issue: i64, // boolean
    }

    #[unique(issue_or_pr, label)]
    pub struct LabelLink {
        pub issue_or_pr: IssuePullRequestShared,
        pub label: Label,
        pub outdated: i64,
    }

    pub struct Label {
        #[unique]
        pub name: String,
        pub description: String,
        pub color: String,
    }

    pub struct Comment {
        #[unique]
        pub comment_id: i64,

        pub author: User,
        pub text: String,

        pub issue_or_pr: IssuePullRequestShared,

        pub created_timestamp: i64,
        pub updated_timestamp: i64,
    }
}

pub use v0::*;

pub fn migrate(db_path: impl AsRef<Path>) -> DatabaseAsync<v0::Schema> {
    let m = Database::migrator(Config::open(db_path))
        .expect("database should not be older than supported versions");

    let db = m
        .finish()
        .expect("database should not be newer than supported versions");

    DatabaseAsync::new(Arc::new(db))
}

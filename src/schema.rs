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

        pub repo: Repo,
    }

    pub struct Assignments {
        pub user: User,
        pub assignment: IssuePullRequestShared,
    }

    pub struct PullRequest {
        #[unique]
        pub shared: IssuePullRequestShared,
        pub draft: i64,
    }

    pub struct Issue {
        #[unique]
        pub shared: IssuePullRequestShared,
    }

    pub struct IssuePrLinks {
        pub from: IssuePullRequestShared,
        pub to: IssuePullRequestShared,
        pub pr_closes_issue: i64, // boolean
    }

    pub struct LabelLinks {
        pub from: IssuePullRequestShared,
        pub to: Label,
    }

    pub struct Label {
        pub name: String,
        pub description: String,
    }

    pub struct Comment {
        pub author: User,
        pub text: String,

        pub created_unix_timestamp: i64,
        pub last_updated_timestamp: i64,
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

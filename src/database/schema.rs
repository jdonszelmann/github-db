use std::{path::Path, sync::Arc};

use octocrab::models::pulls::MergeableState;
use rust_query::{Database, DatabaseAsync, Lazy, migration::schema};

#[schema(Schema)]
#[version(0..=2)]
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

        /// Discriminant of octocrab::models::issues::StateReason,
        /// None if no particular reason
        #[version(1..)]
        pub state_reason: Option<i64>,

        /// None if not closed
        pub closed_at_timestamp: Option<i64>,
        /// None if not closed
        #[version(1..)]
        pub closed_by: Option<User>,

        /// Discriminant of octocrab::models::AuthorAssociation
        #[version(1..)]
        pub author_association: String,

        #[version(..1)]
        pub locked: i64,

        /// None if not locked, Some(empty string) if locked without reason
        #[version(1..)]
        pub lock_reason: Option<String>,

        pub repo: Repo,
    }

    #[unique(user, issue_or_pr)]
    pub struct Assignment {
        pub user: User,
        pub issue_or_pr: IssuePullRequestShared,
        pub outdated: i64,
    }

    #[unique(pr, user)]
    #[version(2..)]
    pub struct ReviewRequest {
        pub user: User,
        pub pr: PullRequest,
        pub outdated: i64,
    }

    pub struct PullRequest {
        #[unique]
        pub shared: IssuePullRequestShared,
        /// bool
        pub draft: i64,
        /// bool
        pub maintainer_can_modify: i64,

        pub num_additions: i64,
        pub num_deletions: i64,
        pub num_changed_files: i64,
        pub num_commits: i64,

        /// none if not merged
        #[version(1..)]
        pub merged_at_timestamp: Option<i64>,
        /// none if not merged
        #[version(1..)]
        pub merge_commit_sha: Option<String>,
        /// none if not merged
        #[version(1..)]
        pub merged_by: Option<User>,

        /// Only none in old database versions, is supposed to always be there
        #[version(1..)]
        pub head_sha: Option<String>,
        /// Only none in old database versions, is supposed to always be there
        #[version(1..)]
        pub base_sha: Option<String>,

        /// Discriminant of octocrab::models::pulls::MergeableState
        #[version(1..)]
        pub mergeable_state: i64,

        /// bool
        #[version(1..)]
        pub mergeable: i64,
        /// bool
        #[version(1..)]
        pub rebaseable: i64,
    }

    pub struct Issue {
        #[unique]
        pub shared: IssuePullRequestShared,
    }

    #[unique(from, to)]
    pub struct IssuePrLink {
        pub from: IssuePullRequestShared,
        pub to: IssuePullRequestShared,
        /// bool
        pub pr_closes_issue: i64,
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

pub use v2::*;

pub fn migrate(db_path: impl AsRef<Path>) -> DatabaseAsync<v2::Schema> {
    let m = Database::migrator(rust_query::migration::Config::open(db_path))
        .expect("database should not be older than supported versions");

    let m = m.migrate(|txn| v0::migrate::Schema {
        issue_pull_request_shared: txn.migrate_ok(|old: Lazy<v0::IssuePullRequestShared>| {
            v0::migrate::IssuePullRequestShared {
                state_reason: None,
                closed_by: None,
                author_association: "Unknown".to_string(),
                lock_reason: (old.locked == 1).then_some("".to_string()),
            }
        }),
        pull_request: txn.migrate_ok(|_: Lazy<v0::PullRequest>| v0::migrate::PullRequest {
            merged_at_timestamp: None,
            merge_commit_sha: None,
            merged_by: None,
            head_sha: None,
            base_sha: None,
            mergeable: 0,
            rebaseable: 0,
            mergeable_state: MergeableState::Unknown as i64,
        }),
    });

    let m = m.migrate(|_txn| v1::migrate::Schema {});

    let db = m
        .finish()
        .expect("database should not be newer than supported versions");

    DatabaseAsync::new(Arc::new(db))
}

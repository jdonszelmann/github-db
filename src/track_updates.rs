use chrono::Utc;
use octocrab::models::{IssueState, issues::Issue, pulls::PullRequest};
use rust_query::{TableRow, Transaction};

use crate::{
    GithubDb, Repo,
    schema::{self, Schema},
};

macro_rules! gen_update {
    ($status: ident) => {
        macro_rules! update {
            ($a: expr, $b: expr) => {{
                let b = $b;
                if $a != b {
                    $status.update(ProcessStatus::Updated);
                    $a = b;
                }
            }};
        }
    };
}

impl GithubDb {
    pub async fn process_pr(
        &self,
        repo: Repo,
        PullRequest {
            url: _,
            id: _,
            node_id: _,
            html_url: _,
            diff_url: _,
            patch_url: _,
            issue_url: _,
            commits_url: _,
            review_comments_url: _,
            review_comment_url: _,
            comments_url: _,
            statuses_url: _,
            number,
            state,
            locked,
            maintainer_can_modify,
            title,
            user,
            body,
            body_text: _,
            body_html: _,
            labels,
            milestone,
            active_lock_reason,
            created_at,
            updated_at,
            closed_at,
            mergeable,
            mergeable_state,
            merged,
            merged_at,
            merged_by,
            merge_commit_sha,
            assignee,
            assignees,
            requested_reviewers,
            requested_teams,
            rebaseable,
            head,
            base,
            links: _,
            author_association,
            draft,
            repo: _,
            additions,
            deletions,
            changed_files,
            commits,
            review_comments,
            comments,
            ..
        }: PullRequest,
    ) -> ProcessStatus {
        self.db
            .transaction_mut_ok(move |txn| {
                use schema::*;

                let mut status = ProcessStatus::Unchanged;

                let Some(author) = user else {
                    tracing::error!("no author for pr #");
                    return ProcessStatus::Unchanged;
                };

                let user = ensure_user_exists(txn, &mut status, *author);

                let repo = txn.find_or_insert(Repo {
                    organization: repo.organization,
                    name: repo.name,
                });
                let closed_at = (state == Some(IssueState::Closed))
                    .then(|| closed_at.unwrap_or_else(Utc::now).timestamp());
                let shared = ensure_shared_exists(
                    &mut *txn,
                    &mut status,
                    user,
                    repo,
                    number,
                    title,
                    body,
                    created_at.unwrap_or_else(Utc::now).timestamp(),
                    updated_at
                        .or(created_at)
                        .unwrap_or_else(Utc::now)
                        .timestamp(),
                    closed_at,
                );

                ensure_pr_exists(txn, &mut status, shared, draft.unwrap_or(false));

                status
            })
            .await
    }

    pub async fn process_issue(
        &self,
        repo: Repo,
        Issue {
            id: _,
            node_id: _,
            url: _,
            repository_url: _,
            labels_url: _,
            comments_url: _,
            events_url: _,
            html_url: _,
            number,
            state,
            state_reason,
            title,
            body,
            body_text: _,
            body_html: _,
            user,
            labels,
            assignee,
            assignees,
            author_association,
            milestone,
            locked,
            active_lock_reason,
            comments,
            pull_request,
            closed_at,
            closed_by,
            created_at,
            updated_at,
            ..
        }: Issue,
    ) -> ProcessStatus {
        self.db
            .transaction_mut_ok(move |txn| {
                use schema::*;

                let mut status = ProcessStatus::Unchanged;

                let user = ensure_user_exists(txn, &mut status, user);

                let repo = txn.find_or_insert(Repo {
                    organization: repo.organization,
                    name: repo.name,
                });
                let closed_at = (state == IssueState::Closed)
                    .then(|| closed_at.unwrap_or_else(Utc::now).timestamp());

                let shared = ensure_shared_exists(
                    txn,
                    &mut status,
                    user,
                    repo,
                    number,
                    Some(title),
                    body,
                    created_at.timestamp(),
                    updated_at.timestamp(),
                    closed_at,
                );

                ensure_issue_exists(txn, &mut status, shared);

                status
            })
            .await
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProcessStatus {
    New,
    Updated,
    Unchanged,
}

impl ProcessStatus {
    fn update(&mut self, other: Self) {
        if (other as usize) < (*self as usize) {
            *self = other;
        }
    }
}

fn ensure_user_exists(
    txn: &mut Transaction<Schema>,
    status: &mut ProcessStatus,
    author: octocrab::models::Author,
) -> TableRow<schema::User> {
    use crate::schema::*;
    gen_update!(status);

    let display_name = author.name.unwrap_or(author.login.clone());
    match txn.insert(User {
        github_id: author.id.0 as i64,
        name: author.login.clone(),
        display_name: display_name.clone(),
    }) {
        Err(e) => {
            let mut user = txn.mutable(e);
            update!(user.name, author.login);
            update!(user.display_name, display_name);
            e
        }
        Ok(i) => {
            status.update(ProcessStatus::New);
            i
        }
    }
}

fn ensure_shared_exists(
    txn: &mut Transaction<Schema>,
    status: &mut ProcessStatus,
    user: TableRow<schema::User>,
    repo: TableRow<schema::Repo>,
    number: u64,
    title: Option<String>,
    body: Option<String>,
    created_timestamp: i64,
    updated_timestamp: i64,
    closed_at_timestamp: Option<i64>,
) -> TableRow<schema::IssuePullRequestShared> {
    use crate::schema::*;
    gen_update!(status);

    match txn.insert(IssuePullRequestShared {
        number: number as i64,
        title: title.clone().unwrap_or_default(),
        description: body.clone().unwrap_or_default(),
        author: user,
        created_timestamp,
        updated_timestamp,
        closed_at_timestamp,
        repo,
    }) {
        Ok(i) => {
            status.update(ProcessStatus::New);
            i
        }
        Err(e) => {
            let mut shared = txn.mutable(e);
            if let Some(title) = title {
                update!(shared.title, title);
            }
            if let Some(body) = body {
                update!(shared.description, body);
            }
            update!(shared.author, user);
            update!(shared.created_timestamp, created_timestamp);
            update!(shared.updated_timestamp, updated_timestamp);
            update!(shared.closed_at_timestamp, closed_at_timestamp);
            e
        }
    }
}

fn ensure_pr_exists(
    txn: &mut Transaction<Schema>,
    status: &mut ProcessStatus,
    shared: TableRow<schema::IssuePullRequestShared>,
    draft: bool,
) -> TableRow<schema::PullRequest> {
    use crate::schema::*;
    gen_update!(status);
    match txn.insert(PullRequest {
        shared,
        draft: draft as i64,
    }) {
        Ok(i) => {
            status.update(ProcessStatus::New);
            i
        }
        Err(e) => {
            let mut pr = txn.mutable(e);
            update!(pr.draft, draft as i64);
            e
        }
    }
}

fn ensure_issue_exists(
    txn: &mut Transaction<Schema>,
    status: &mut ProcessStatus,
    shared: TableRow<schema::IssuePullRequestShared>,
) -> TableRow<schema::Issue> {
    use crate::schema::*;
    gen_update!(status);

    match txn.insert(Issue { shared }) {
        Ok(i) => {
            status.update(ProcessStatus::New);
            i
        }
        Err(e) => {
            let _issue = txn.mutable(e);
            e
        }
    }
}

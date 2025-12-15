use chrono::Utc;
use octocrab::models::{
    AuthorAssociation, IssueState, Label,
    issues::{Comment, Issue, IssueStateReason},
    pulls::{MergeableState, PullRequest},
};
use rust_query::{TableRow, Transaction};

use crate::{
    GithubDb, Repo,
    database::schema::{self, Schema},
};

macro_rules! gen_update {
    ($status: ident) => {
        macro_rules! update {
            (tracked: $a: expr, $b: expr) => {{
                let b = $b;
                if $a != b {
                    $status.update(ProcessStatus::Updated);
                    $a = b;
                }
            }};
            ($a: expr, $b: expr) => {{
                $a = $b;
            }};
        }
    };
}

impl GithubDb {
    pub async fn process_comment(
        &self,
        _repo: Repo,
        Comment {
            id,
            node_id: _,
            url: _,
            html_url: _,
            issue_url: _,
            body,
            body_text: _,
            body_html: _,
            author_association: _,
            user,
            created_at,
            updated_at,
            ..
        }: Comment,
        issue_number: u64,
    ) -> ProcessStatus {
        self.db
            .transaction_mut_ok(move |txn| {
                use schema::*;
                let mut status = ProcessStatus::Unchanged;

                let Some(issue_or_pr) =
                    txn.query_one(IssuePullRequestShared.number(issue_number as i64))
                else {
                    tracing::error!("no issue found in database for comment {}", id);
                    return status;
                };

                let author = ensure_user_exists(txn, &mut status, user);
                ensure_comment_exists(
                    txn,
                    &mut status,
                    *id as i64,
                    author,
                    issue_or_pr,
                    body,
                    created_at.timestamp(),
                    updated_at.unwrap_or(created_at).timestamp(),
                );

                status
            })
            .await
    }

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
            milestone: _,
            active_lock_reason,
            created_at,
            updated_at,
            closed_at,
            mergeable,
            mergeable_state,
            merged: _,
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
            review_comments: _,
            comments: _,
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

                let closed_by = merged_by.map(|user| ensure_user_exists(txn, &mut status, *user));

                let shared = ensure_shared_exists(
                    &mut *txn,
                    &mut status,
                    user,
                    repo,
                    number,
                    title,
                    body,
                    locked.then_some(active_lock_reason).flatten(),
                    created_at.unwrap_or_else(Utc::now).timestamp(),
                    updated_at
                        .or(created_at)
                        .unwrap_or_else(Utc::now)
                        .timestamp(),
                    closed_at,
                    None,
                    closed_by,
                    author_association,
                );

                ensure_pr_exists(
                    txn,
                    &mut status,
                    shared,
                    draft.unwrap_or(false),
                    maintainer_can_modify,
                    additions.unwrap_or_default() as i64,
                    deletions.unwrap_or_default() as i64,
                    changed_files.unwrap_or_default() as i64,
                    commits.unwrap_or_default() as i64,
                    merged_at.map(|i| i.timestamp()),
                    merge_commit_sha,
                    closed_by,
                    head.sha,
                    base.sha,
                    mergeable.unwrap_or(false),
                    rebaseable.unwrap_or(false),
                    mergeable_state.unwrap_or(MergeableState::Unknown),
                );

                let labels: Vec<_> = labels
                    .unwrap_or_default()
                    .into_iter()
                    .map(|label| ensure_label_exists(txn, &mut status, label))
                    .collect();
                let outdated_labels = update_label_assignments(txn, &mut status, shared, labels);

                let assigned_users: Vec<_> = assignees
                    .unwrap_or(assignee.map(|i| *i).as_slice().to_vec())
                    .into_iter()
                    .map(|user| ensure_user_exists(txn, &mut status, user))
                    .collect();

                let outdated_assignments =
                    update_assignments(txn, &mut status, shared, assigned_users);

                let txn = txn.downgrade();
                for i in outdated_assignments {
                    if let Err(()) = txn.delete(i) {
                        tracing::error!("assignment {i:?} referenced somehow");
                    }
                }
                for i in outdated_labels {
                    if let Err(()) = txn.delete(i) {
                        tracing::error!("label assignment {i:?} referenced somehow");
                    }
                }

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
            assignee: _,
            assignees,
            author_association,
            milestone: _,
            locked,
            active_lock_reason,
            comments: _,
            pull_request: _,
            closed_at,
            closed_by,
            created_at,
            updated_at,
            ..
        }: Issue,
    ) -> ProcessStatus {
        let status = self
            .db
            .transaction_mut_ok({
                let repo = repo.clone();
                move |txn| {
                    use schema::*;

                    let mut status = ProcessStatus::Unchanged;

                    let user = ensure_user_exists(txn, &mut status, user);

                    let repo = txn.find_or_insert(Repo {
                        organization: repo.organization,
                        name: repo.name,
                    });
                    let closed_at = (state == IssueState::Closed)
                        .then(|| closed_at.unwrap_or_else(Utc::now).timestamp());

                    let closed_by =
                        closed_by.map(|user| ensure_user_exists(txn, &mut status, user));

                    let shared = ensure_shared_exists(
                        txn,
                        &mut status,
                        user,
                        repo,
                        number,
                        Some(title),
                        body,
                        locked.then_some(active_lock_reason).flatten(),
                        created_at.timestamp(),
                        updated_at.timestamp(),
                        closed_at,
                        state_reason,
                        closed_by,
                        author_association,
                    );

                    ensure_issue_exists(txn, &mut status, shared);

                    let labels: Vec<_> = labels
                        .into_iter()
                        .map(|label| ensure_label_exists(txn, &mut status, label))
                        .collect();
                    let outdated_labels =
                        update_label_assignments(txn, &mut status, shared, labels);

                    let assigned_users: Vec<_> = assignees
                        .into_iter()
                        .map(|user| ensure_user_exists(txn, &mut status, user))
                        .collect();
                    let outdated_assignments =
                        update_assignments(txn, &mut status, shared, assigned_users);

                    let txn = txn.downgrade();
                    for i in outdated_assignments {
                        if let Err(()) = txn.delete(i) {
                            tracing::error!("assignment {i:?} referenced somehow");
                        }
                    }
                    for i in outdated_labels {
                        if let Err(()) = txn.delete(i) {
                            tracing::error!("label assignment {i:?} referenced somehow");
                        }
                    }

                    status
                }
            })
            .await;

        self.add_comments_updated_req(status, repo, Some(updated_at.timestamp()), number)
            .await;
        status
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

fn ensure_label_exists(
    txn: &mut Transaction<Schema>,
    status: &mut ProcessStatus,

    Label {
        name,
        description,
        color,
        id: _,
        node_id: _,
        url: _,
        default: _,
        ..
    }: Label,
) -> TableRow<schema::Label> {
    use crate::schema::*;
    gen_update!(status);

    match txn.insert(Label {
        name,
        description: description.clone().unwrap_or_default(),
        color: color.clone(),
    }) {
        Err(e) => {
            let mut label = txn.mutable(e);
            update!(label.color, color);
            if let Some(description) = description {
                update!(label.description, description);
            }
            e
        }
        Ok(i) => {
            status.update(ProcessStatus::Updated);
            i
        }
    }
}

fn ensure_comment_exists(
    txn: &mut Transaction<Schema>,
    status: &mut ProcessStatus,
    comment_id: i64,
    author: TableRow<schema::User>,
    issue_or_pr: TableRow<schema::IssuePullRequestShared>,
    text: Option<String>,
    created_timestamp: i64,
    updated_timestamp: i64,
) -> TableRow<schema::Comment> {
    use crate::schema::*;
    gen_update!(status);

    match txn.insert(Comment {
        comment_id,
        author,
        text: text.clone().unwrap_or_default(),
        issue_or_pr,
        created_timestamp,
        updated_timestamp,
    }) {
        Err(e) => {
            let mut comment = txn.mutable(e);
            update!(comment.author, author);
            if let Some(text) = text {
                update!(comment.text, text);
            }
            // don't issue pr, it can't change (I hope)
            update!(comment.created_timestamp, created_timestamp);
            update!(tracked: comment.updated_timestamp, updated_timestamp);
            e
        }
        Ok(i) => {
            status.update(ProcessStatus::New);
            i
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
            status.update(ProcessStatus::Updated);
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
    lock_reason: Option<String>,
    created_timestamp: i64,
    updated_timestamp: i64,
    closed_at_timestamp: Option<i64>,
    state_reason: Option<IssueStateReason>,
    closed_by: Option<TableRow<schema::User>>,
    author_association: Option<AuthorAssociation>,
) -> TableRow<schema::IssuePullRequestShared> {
    use crate::schema::*;
    gen_update!(status);

    let state_reason = state_reason.map(|i| i as i64);

    let association_given = author_association.is_some();
    let author_association = match author_association.unwrap_or(AuthorAssociation::None) {
        AuthorAssociation::Collaborator => "Collaborator".to_string(),
        AuthorAssociation::Contributor => "Contributor".to_string(),
        AuthorAssociation::FirstTimer => "FirstTimer".to_string(),
        AuthorAssociation::FirstTimeContributor => "FirstTimeContributor".to_string(),
        AuthorAssociation::Mannequin => "Mannequin".to_string(),
        AuthorAssociation::Member => "Member".to_string(),
        AuthorAssociation::None => "None".to_string(),
        AuthorAssociation::Owner => "Owner".to_string(),
        AuthorAssociation::Other(o) => o,
        _ => "Unknown".to_string(),
    };

    match txn.insert(IssuePullRequestShared {
        number: number as i64,
        title: title.clone().unwrap_or_default(),
        description: body.clone().unwrap_or_default(),
        author: user,
        lock_reason: lock_reason.clone(),
        created_timestamp,
        updated_timestamp,
        closed_at_timestamp,
        repo,
        state_reason,
        closed_by,
        author_association: author_association.clone(),
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
            update!(shared.lock_reason, lock_reason);
            update!(shared.author, user);
            update!(shared.created_timestamp, created_timestamp);
            update!(tracked: shared.updated_timestamp, updated_timestamp);
            update!(shared.closed_at_timestamp, closed_at_timestamp);
            update!(shared.state_reason, state_reason);
            update!(shared.closed_by, closed_by);

            if association_given {
                update!(shared.author_association, author_association);
            }
            e
        }
    }
}

fn update_assignments(
    txn: &mut Transaction<Schema>,
    status: &mut ProcessStatus,
    shared: TableRow<schema::IssuePullRequestShared>,
    users: Vec<TableRow<schema::User>>,
) -> Vec<TableRow<schema::Assignment>> {
    use crate::schema::*;
    gen_update!(status);

    let assignments_for_shared = txn.query(|rows| {
        let assignments = rows.join(Assignment);
        rows.filter(assignments.issue_or_pr.eq(shared));
        rows.into_vec(assignments)
    });

    for i in assignments_for_shared {
        txn.mutable(i).outdated = 1;
    }

    for user in users {
        match txn.insert(Assignment {
            user,
            issue_or_pr: shared,
            outdated: 0,
        }) {
            Ok(i) => {
                status.update(ProcessStatus::New);
                i
            }
            Err(e) => {
                let mut assignment = txn.mutable(e);
                update!(assignment.outdated, 0);
                e
            }
        };
    }

    txn.query(|rows| {
        let assignments = rows.join(Assignment);
        rows.filter(assignments.issue_or_pr.eq(shared));
        rows.filter(assignments.outdated.eq(1));
        rows.into_vec(assignments)
    })
}

fn update_label_assignments(
    txn: &mut Transaction<Schema>,
    _status: &mut ProcessStatus,
    shared: TableRow<schema::IssuePullRequestShared>,
    labels: Vec<TableRow<schema::Label>>,
) -> Vec<TableRow<schema::Assignment>> {
    use crate::schema::*;
    gen_update!(status);

    let assignments_for_shared = txn.query(|rows| {
        let assignments = rows.join(LabelLink);
        rows.filter(assignments.issue_or_pr.eq(shared));
        rows.into_vec(assignments)
    });

    for i in assignments_for_shared {
        txn.mutable(i).outdated = 1;
    }

    for label in labels {
        match txn.insert(LabelLink {
            label,
            issue_or_pr: shared,
            outdated: 0,
        }) {
            Ok(i) => {
                // status.update(ProcessStatus::New);
                i
            }
            Err(e) => {
                let mut assignment = txn.mutable(e);
                update!(assignment.outdated, 0);
                e
            }
        };
    }

    txn.query(|rows| {
        let assignments = rows.join(Assignment);
        rows.filter(assignments.issue_or_pr.eq(shared));
        rows.filter(assignments.outdated.eq(1));
        rows.into_vec(assignments)
    })
}

fn ensure_pr_exists(
    txn: &mut Transaction<Schema>,
    status: &mut ProcessStatus,
    shared: TableRow<schema::IssuePullRequestShared>,
    draft: bool,
    maintainer_can_modify: bool,
    num_additions: i64,
    num_deletions: i64,
    num_changed_files: i64,
    num_commits: i64,
    merged_at_timestamp: Option<i64>,
    merge_commit_sha: Option<String>,
    merged_by: Option<TableRow<schema::User>>,
    head_sha: String,
    base_sha: String,
    mergeable: bool,
    rebaseable: bool,
    mergeable_state: MergeableState,
) -> TableRow<schema::PullRequest> {
    use crate::schema::*;
    gen_update!(status);
    match txn.insert(PullRequest {
        shared,
        draft: draft as i64,
        maintainer_can_modify: maintainer_can_modify as i64,
        num_additions,
        num_deletions,
        num_changed_files,
        num_commits,
        merged_at_timestamp: merged_at_timestamp,
        merge_commit_sha: merge_commit_sha.clone(),
        merged_by: merged_by,
        head_sha: Some(head_sha.clone()),
        base_sha: Some(base_sha.clone()),
        mergeable: mergeable as i64,
        rebaseable: rebaseable as i64,
        mergeable_state: mergeable_state.clone() as i64,
    }) {
        Ok(i) => {
            status.update(ProcessStatus::New);
            i
        }
        Err(e) => {
            let mut pr = txn.mutable(e);
            update!(pr.draft, draft as i64);
            update!(pr.maintainer_can_modify, maintainer_can_modify as i64);
            update!(pr.num_additions, num_additions);
            update!(pr.num_deletions, num_deletions);
            update!(pr.num_changed_files, num_changed_files);
            update!(pr.num_commits, num_commits);
            update!(pr.merged_at_timestamp, merged_at_timestamp);
            update!(pr.merge_commit_sha, merge_commit_sha);
            update!(pr.merged_by, merged_by);
            update!(pr.head_sha, Some(head_sha));
            update!(pr.base_sha, Some(base_sha));
            update!(pr.mergeable, mergeable as i64);
            update!(pr.rebaseable, rebaseable as i64);
            update!(pr.mergeable_state, mergeable_state as i64);
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
    // gen_update!(status);

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

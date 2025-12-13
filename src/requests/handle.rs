use crate::{
    GithubDb, ProcessStatus, Repo,
    requests::{ListType, Priority, Request},
};
use std::str::FromStr;

use chrono::{DateTime, Utc};
use http::Uri;
use octocrab::{Page, params::Direction};

macro_rules! build_request {
    ($_self: tt, $url: ident, $repo: ident $($other_args: ident)*) => {
        macro_rules! request {
            ($e: expr) => {{
                let page: Result<Page<_>, _> = if let Some(page) = $url
                    && let Ok(i) = Uri::from_str(&page)
                {
                    let Some(page) = $_self.octocrab().await.get_page(&Some(i)).await.transpose() else {
                        return;
                    };
                    page
                } else {
                    $e
                };

                match page {
                    Ok(mut i) => (i.take_items(), i.next),
                    Err(e) => {
                        tracing::error!("{e:?}");
                        return;
                    }
                }
            }};
        }

        macro_rules! iter {
            ($items: ident, $method: ident) => {{
                let mut any_updated = false;
                for issue in $items {
                    if !matches!(
                        $_self.$method($repo.clone(), issue, $($other_args),*).await,
                        ProcessStatus::Unchanged
                    ) {
                        any_updated = true;
                    }
                }
                any_updated
            }};
        }
    };
}

impl GithubDb {
    async fn handle_list_prs(
        &self,
        repo: Repo,
        page_num: usize,
        url: Option<String>,
        list_type: ListType,
    ) {
        build_request!(self, url, repo);
        let (items, next) = request!(
            self.octocrab()
                .await
                .pulls(&repo.organization, &repo.name)
                .list()
                .sort(octocrab::params::pulls::Sort::Updated)
                .direction(match list_type {
                    ListType::New => Direction::Descending,
                    ListType::Old => Direction::Ascending,
                })
                .state(octocrab::params::State::All)
                .page(page_num as u32)
                .per_page(100)
                .send()
                .await
        );

        tracing::debug!("processing {} {list_type} pulls", items.len());
        let any_updated = iter!(items, process_pr);
        let next_page_num = if next.is_some() { page_num + 1 } else { 0 };

        match (list_type, any_updated) {
            (ListType::New, true) => {
                self.add_req(
                    Priority::Update,
                    Request::NewPr {
                        repo,
                        page: next_page_num,
                        url: next.map(|i| i.to_string()),
                    },
                )
                .await;
            }
            (ListType::Old, updated) => {
                self.add_req(
                    if updated {
                        Priority::Update
                    } else {
                        Priority::Index
                    },
                    Request::OldPr {
                        repo,
                        page: next_page_num,
                        url: next.map(|i| i.to_string()),
                    },
                )
                .await;
            }
            _ => {}
        }
    }

    async fn handle_list_issues(
        &self,
        repo: Repo,
        page_num: usize,
        url: Option<String>,
        list_type: ListType,
    ) {
        build_request!(self, url, repo);
        let (items, next) = request!(
            self.octocrab()
                .await
                .issues(&repo.organization, &repo.name)
                .list()
                .sort(octocrab::params::issues::Sort::Updated)
                .direction(match list_type {
                    ListType::New => Direction::Descending,
                    ListType::Old => Direction::Ascending,
                })
                .state(octocrab::params::State::All)
                .page(page_num as u32)
                .per_page(100)
                .send()
                .await
        );

        tracing::debug!("processing {} {list_type} issues", items.len());
        let any_updated = iter!(items, process_issue);

        let next_page_num = if next.is_some() { page_num + 1 } else { 0 };

        match (list_type, any_updated) {
            (ListType::New, true) => {
                self.add_req(
                    Priority::Update,
                    Request::NewIssue {
                        repo,
                        page: next_page_num,
                        url: next.map(|i| i.to_string()),
                    },
                )
                .await;
            }
            (ListType::Old, updated) => {
                self.add_req(
                    if updated {
                        Priority::Update
                    } else {
                        Priority::Index
                    },
                    Request::OldIssue {
                        repo,
                        page: next_page_num,
                        url: next.map(|i| i.to_string()),
                    },
                )
                .await;
            }
            _ => {}
        }
    }

    async fn handle_list_comments(
        &self,
        repo: Repo,
        issue_number: u64,
        since_timestamp: Option<i64>,
        page_num: usize,
        url: Option<String>,
    ) {
        build_request!(self, url, repo issue_number);
        let (items, next) = request!({
            let octocrab = self.octocrab().await;
            let comments = octocrab.issues(&repo.organization, &repo.name);
            let mut comments = comments.list_comments(issue_number);

            if let Some(since) = since_timestamp
                && let Some(stamp) = DateTime::<Utc>::from_timestamp_secs(since - 100)
            {
                // - 100 for some leaway
                comments = comments.since(stamp);
            }

            comments.page(page_num as u32).per_page(100).send().await
        });

        tracing::debug!("processing {} comments", items.len());
        let any_updated = iter!(items, process_comment);

        if any_updated && let Some(next) = next {
            self.add_req(
                Priority::Comments,
                Request::Comments {
                    repo,
                    issue_number,
                    since_timestamp,
                    page: page_num + 1,
                    url: Some(next.to_string()),
                },
            )
            .await;
        }
    }

    pub async fn handle_request(&self, r: Request) {
        tracing::debug!("{r:?}");
        tracing::info!("handling request {}", r.name());
        match r {
            Request::OldPr { repo, page, url } => {
                self.handle_list_prs(repo, page, url, ListType::Old).await
            }
            Request::NewPr { repo, page, url } => {
                self.handle_list_prs(repo, page, url, ListType::New).await
            }
            Request::OldIssue { repo, page, url } => {
                self.handle_list_issues(repo, page, url, ListType::Old)
                    .await
            }
            Request::NewIssue { repo, page, url } => {
                self.handle_list_issues(repo, page, url, ListType::New)
                    .await
            }
            Request::Comments {
                repo,
                issue_number,
                since_timestamp,
                page,
                url,
            } => {
                self.handle_list_comments(repo, issue_number, since_timestamp, page, url)
                    .await
            }
        }
    }
}

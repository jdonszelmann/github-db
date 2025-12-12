use std::{
    fmt::{Debug, Display},
    future::poll_fn,
    path::Path,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    task::Poll,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Utc};
use http::Uri;
use octocrab::{
    Octocrab, Page,
    models::{IssueState, issues::Issue, pulls::PullRequest},
    params::Direction,
};
use rust_query::{DatabaseAsync, TableRow, Transaction, aggregate};
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task, time::interval};

use crate::{schema::Schema, track_updates::ProcessStatus};

mod schema;
mod track_updates;

#[derive(Serialize, Deserialize, Clone)]
pub struct Repo {
    pub organization: String,
    pub name: String,
}

impl Debug for Repo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.organization, self.name)
    }
}

impl FromStr for Repo {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (before, after) = s.split_once("/").ok_or(())?;

        Ok(Repo {
            organization: before.to_string(),
            name: after.to_string(),
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Priority {
    // high prioriry, when things changed!
    Update = 0,
    // very low priority, tries to visit all items regularly
    Index,
    Other,
}

impl Priority {
    const ALL: [Priority; 3] = [Self::Update, Self::Other, Self::Index];

    fn fraction(&self) -> f64 {
        // must add to 1.0
        match self {
            Priority::Update => 0.6,
            Priority::Other => 0.2,
            Priority::Index => 0.1,
        }
    }
}

pub struct RequestLimits {
    global_limit: usize,
    category_limits: [(f64, Instant); Priority::ALL.len()],
    saved_up: f64,
}

impl Display for RequestLimits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut res = f.debug_struct("RequestLimits");

        for i in Priority::ALL {
            res.field(&format!("{i:?}"), &self.category_limits[i as usize].0);
        }

        res.field("saved-up", &self.saved_up);

        res.finish()
    }
}

impl RequestLimits {
    pub fn new(limit: usize) -> Self {
        Self {
            global_limit: limit,
            category_limits: Priority::ALL.map(|_| (0.0, Instant::now())),
            saved_up: 0.0,
        }
    }

    pub async fn update(&mut self, next_request: impl AsyncFn(Priority) -> bool) {
        let mut saved_up = self.saved_up;

        for category in Priority::ALL {
            // The limit is in requests per hour.
            const LIMIT_DURATION: Duration = Duration::from_secs(3600);

            let now = Instant::now();
            let (before_count, before_time) = &mut self.category_limits[category as usize];
            let elapsed = now.duration_since(*before_time);

            let new_requests_allowed = (elapsed.as_secs_f64() / LIMIT_DURATION.as_secs_f64())
                * category.fraction()
                * self.global_limit as f64;

            *before_time = now;
            *before_count += new_requests_allowed + saved_up;
            saved_up = 0.0;

            while *before_count >= 1.0 {
                if next_request(category).await {
                    *before_count -= 1.0;
                } else {
                    break;
                }
            }

            let limit = 0.5 * self.global_limit as f64 * category.fraction();
            if *before_count >= limit {
                saved_up = *before_count - limit;
                *before_count = limit;
            }
        }

        self.saved_up = saved_up;
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    /// List oldest PRs. If an old PR page changed,
    /// then we must have not indexed much yet.
    /// Spend some `Update` budget on indexing them all until we find a page
    /// on which we've already indexed everything. Otherwise, use `Index` priority
    /// to step through pages anyway to make sure we've not missed anything.
    ///
    /// This gets issued at startup if no OldPr requests are in the queue.
    OldPr {
        repo: Repo,
        page: usize,
        url: Option<String>,
    },
    /// List new PRs. If anything changed on the page,
    /// immediately list more pages until we find one on which no PRs changed.
    ///
    /// Gets issued regularly at `Update` priority to update new prs.
    NewPr {
        repo: Repo,
        page: usize,
        url: Option<String>,
    },
    NewIssue {
        repo: Repo,
        page: usize,
        url: Option<String>,
    },
    OldIssue {
        repo: Repo,
        page: usize,
        url: Option<String>,
    },
}
impl Request {
    fn name(&self) -> &'static str {
        match self {
            Request::OldPr { .. } => "OldPr",
            Request::NewPr { .. } => "NewPr",
            Request::NewIssue { .. } => "NewIssue",
            Request::OldIssue { .. } => "OldIssue",
        }
    }
}

pub struct GithubDb {
    db: DatabaseAsync<Schema>,
    octocrab: Octocrab,

    limits: Mutex<RequestLimits>,
    request_sequence_number: AtomicI64,

    refresh: Mutex<tokio::time::Interval>,

    repos: Vec<Repo>,
}

impl GithubDb {
    pub async fn new(
        db_path: impl AsRef<Path>,
        github_app_id: impl AsRef<str>,
        github_app_secret: impl AsRef<str>,
        requests_per_hour: usize,
        repos: &[&str],
    ) -> Self {
        let octocrab = octocrab::Octocrab::builder()
            // .personal_token(github_app_secret.as_ref().to_string())
            .basic_auth(
                github_app_id.as_ref().to_string(),
                github_app_secret.as_ref().to_string(),
            )
            .build()
            .unwrap();
        let db = schema::migrate(db_path);

        let max_seq_number = db
            .transaction_mut_ok(|txn| {
                use schema::*;
                txn.query_one(aggregate(|rows| {
                    let queue = rows.join(Request);
                    rows.max(&queue.sequence_number)
                }))
                .unwrap_or(0)
            })
            .await
            + 1;

        let res = Self {
            db,
            octocrab,
            repos: repos
                .iter()
                .map(|f| {
                    Repo::from_str(f)
                        .map_err(|()| format!("couldn't parse {f} as repository"))
                        .unwrap()
                })
                .collect(),
            limits: Mutex::new(RequestLimits::new(requests_per_hour)),
            request_sequence_number: AtomicI64::new(max_seq_number),
            refresh: Mutex::new(interval(Duration::from_secs(60))),
        };

        res.startup_requests().await;

        res
    }

    async fn startup_requests(&self) {
        for repo in &self.repos {
            let oldpr = Request::OldPr {
                repo: repo.clone(),
                page: 0,
                url: None,
            };
            let oldpr_name = oldpr.name();

            let num_oldpr = self
                .db
                .transaction(move |txn| {
                    txn.query_one(aggregate(|row| {
                        use schema::*;
                        let r = row.join(Request);
                        row.filter(r.name.eq(oldpr_name));
                        row.count_distinct(r)
                    }))
                })
                .await;

            tracing::debug!("number of old pr requests in queue: {num_oldpr}");
            if num_oldpr == 0 {
                self.add_request(Priority::Index, oldpr).await;
            }

            let oldissue = Request::OldIssue {
                repo: repo.clone(),
                page: 0,
                url: None,
            };
            let oldissue_name = oldissue.name();

            let num_oldpr = self
                .db
                .transaction(move |txn| {
                    txn.query_one(aggregate(|row| {
                        use schema::*;
                        let r = row.join(Request);
                        row.filter(r.name.eq(oldissue_name));
                        row.count_distinct(r)
                    }))
                })
                .await;

            tracing::debug!("number of old issue requests in queue: {num_oldpr}");
            if num_oldpr == 0 {
                self.add_request(Priority::Index, oldissue).await;
            }

            self.add_request(
                Priority::Update,
                Request::NewPr {
                    repo: repo.clone(),
                    page: 0,
                    url: None,
                },
            )
            .await;
            self.add_request(
                Priority::Update,
                Request::NewIssue {
                    repo: repo.clone(),
                    page: 0,
                    url: None,
                },
            )
            .await;
        }
    }

    async fn refresh(&self) {
        for repo in &self.repos {
            self.add_request(
                Priority::Update,
                Request::NewPr {
                    repo: repo.clone(),
                    page: 0,
                    url: None,
                },
            )
            .await;
            self.add_request(
                Priority::Update,
                Request::NewIssue {
                    repo: repo.clone(),
                    page: 0,
                    url: None,
                },
            )
            .await;
        }
    }

    /// Call this in your main loop
    pub async fn update(self: Arc<Self>) {
        let mut refresh = self.refresh.lock().await;
        if poll_fn(|cx| match refresh.poll_tick(cx) {
            Poll::Ready(r) => Poll::Ready(Some(r)),
            Poll::Pending => Poll::Ready(None),
        })
        .await
        .is_some()
        {
            self.refresh().await;
        }

        self.limits
            .lock()
            .await
            .update(async |c| {
                if let Some(r) = self.next_request(c).await {
                    let this = self.clone();
                    task::spawn(async move {
                        this.do_request(r).await;
                    });
                    true
                } else {
                    tracing::debug!("no request for category {c:?}");
                    false
                }
            })
            .await;

        tracing::debug!("{}", self.limits.lock().await);
        self.stats().await;
    }

    async fn stats(&self) {
        let num_prs = self
            .db
            .transaction(move |txn| {
                txn.query_one(aggregate(|row| {
                    use schema::*;
                    let r = row.join(PullRequest);
                    row.count_distinct(r)
                }))
            })
            .await;
        let num_issues = self
            .db
            .transaction(move |txn| {
                txn.query_one(aggregate(|row| {
                    use schema::*;
                    let r = row.join(Issue);
                    row.count_distinct(r)
                }))
            })
            .await;
        let num_shared = self
            .db
            .transaction(move |txn| {
                txn.query_one(aggregate(|row| {
                    use schema::*;
                    let r = row.join(IssuePullRequestShared);
                    row.count_distinct(r)
                }))
            })
            .await;
        let num_users = self
            .db
            .transaction(move |txn| {
                txn.query_one(aggregate(|row| {
                    use schema::*;
                    let r = row.join(User);
                    row.count_distinct(r)
                }))
            })
            .await;

        let num_requests = self
            .db
            .transaction(move |txn| {
                txn.query_one(aggregate(|row| {
                    use schema::*;
                    let r = row.join(Request);
                    row.count_distinct(r)
                }))
            })
            .await;

        tracing::info!(
            "prs: {num_prs} issues: {num_issues} shared: {num_shared} users: {num_users} pending requests: {num_requests}"
        );
    }

    async fn next_request(&self, c: Priority) -> Option<Request> {
        loop {
            let data = self
                .db
                .transaction_mut_ok(move |txn| {
                    use schema::*;

                    let req = txn.query_one(aggregate(|rows| {
                        let request = rows.join(Request);
                        rows.filter(request.category.eq(c as i64));

                        let min_seq = rows.min(&request.sequence_number);
                        let min_seq = rows.filter_some(min_seq);
                        rows.filter(min_seq.eq(&request.sequence_number));
                        rows.min(request)
                    }))?;

                    let data = &txn.lazy(req).data;
                    let request_data = serde_json::from_slice(data);

                    let txn = txn.downgrade();
                    txn.delete(req).expect("already deleted");

                    Some(request_data)
                })
                .await?;

            match data {
                Err(e) => {
                    println!("error: {e}");
                }
                Ok(i) => break i,
            }
        }
    }

    async fn add_request(&self, c: Priority, r: Request) {
        tracing::debug!("add request: {r:?} at p {c:?}");
        let data = serde_json::to_vec(&r).unwrap();
        let name = r.name();

        let sequence_number = self.request_sequence_number.fetch_add(1, Ordering::Relaxed);

        self.db
            .transaction_mut_ok(move |txn| {
                use schema::*;

                txn.insert(Request {
                    name,
                    category: c as i64,
                    sequence_number,
                    data,
                })
                .expect("duplicate sequence number");
            })
            .await
    }

    async fn do_list_prs(
        &self,
        repo: Repo,
        page_num: usize,
        url: Option<String>,
        list_type: ListType,
    ) {
        let page: Result<Page<PullRequest>, _> = if let Some(page) = url
            && let Ok(i) = Uri::from_str(&page)
        {
            let Some(page) = self.octocrab.get_page(&Some(i)).await.transpose() else {
                return;
            };
            page
        } else {
            self.octocrab
                .pulls(&repo.organization, &repo.name)
                .list()
                .sort(octocrab::params::pulls::Sort::Updated)
                .direction(match list_type {
                    ListType::New => Direction::Descending,
                    ListType::Old => Direction::Ascending,
                })
                .page(page_num as u32)
                .per_page(100)
                .send()
                .await
        };

        let (items, next) = match page {
            Ok(mut i) => (i.take_items(), i.next),
            Err(e) => {
                tracing::error!("{e:?}");
                return;
            }
        };

        let mut any_updated = false;
        tracing::debug!("processing {} new prs", items.len());
        for pr in items {
            if !matches!(
                self.process_pr(repo.clone(), pr).await,
                ProcessStatus::Unchanged
            ) {
                any_updated = true;
            }
        }

        if any_updated {
            match (list_type, any_updated) {
                (ListType::New, true) => {
                    self.add_request(
                        Priority::Update,
                        Request::NewPr {
                            repo,
                            page: page_num + 1,
                            url: next.map(|i| i.to_string()),
                        },
                    )
                    .await;
                }
                (ListType::Old, updated) => {
                    self.add_request(
                        if updated {
                            Priority::Update
                        } else {
                            Priority::Index
                        },
                        Request::OldPr {
                            repo,
                            page: page_num + 1,
                            url: next.map(|i| i.to_string()),
                        },
                    )
                    .await;
                }
                _ => {}
            }
        }
    }

    async fn do_list_issues(
        &self,
        repo: Repo,
        page_num: usize,
        url: Option<String>,
        list_type: ListType,
    ) {
        let page: Result<Page<Issue>, _> = if let Some(page) = url
            && let Ok(i) = Uri::from_str(&page)
        {
            let Some(page) = self.octocrab.get_page(&Some(i)).await.transpose() else {
                return;
            };
            page
        } else {
            self.octocrab
                .issues(&repo.organization, &repo.name)
                .list()
                .sort(octocrab::params::issues::Sort::Updated)
                .direction(match list_type {
                    ListType::New => Direction::Descending,
                    ListType::Old => Direction::Ascending,
                })
                .page(page_num as u32)
                .per_page(100)
                .send()
                .await
        };

        let (items, next) = match page {
            Ok(mut i) => (i.take_items(), i.next),
            Err(e) => {
                tracing::error!("{e:?}");
                return;
            }
        };

        let mut any_updated = false;
        tracing::debug!("processing {} {list_type} issues", items.len());
        for issue in items {
            if !matches!(
                self.process_issue(repo.clone(), issue).await,
                ProcessStatus::Unchanged
            ) {
                any_updated = true;
            }
        }

        if !any_updated {
            tracing::debug!("nothing changed")
        }

        if any_updated {
            match (list_type, any_updated) {
                (ListType::New, true) => {
                    self.add_request(
                        Priority::Update,
                        Request::NewIssue {
                            repo,
                            page: page_num + 1,
                            url: next.map(|i| i.to_string()),
                        },
                    )
                    .await;
                }
                (ListType::Old, updated) => {
                    self.add_request(
                        if updated {
                            Priority::Update
                        } else {
                            Priority::Index
                        },
                        Request::OldIssue {
                            repo,
                            page: page_num + 1,
                            url: next.map(|i| i.to_string()),
                        },
                    )
                    .await;
                }
                _ => {}
            }
        }
    }

    async fn do_request(&self, r: Request) {
        tracing::debug!("handling request {r:?}");
        match r {
            Request::OldPr { repo, page, url } => {
                self.do_list_prs(repo, page, url, ListType::Old).await
            }
            Request::NewPr { repo, page, url } => {
                self.do_list_prs(repo, page, url, ListType::New).await
            }
            Request::OldIssue { repo, page, url } => {
                self.do_list_issues(repo, page, url, ListType::Old).await
            }
            Request::NewIssue { repo, page, url } => {
                self.do_list_issues(repo, page, url, ListType::New).await
            }
        }
    }
}

pub enum ListType {
    New,
    Old,
}

impl Display for ListType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ListType::New => write!(f, "new"),
            ListType::Old => write!(f, "old"),
        }
    }
}

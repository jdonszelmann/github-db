use std::{
    collections::VecDeque,
    fmt::Debug,
    future::poll_fn,
    path::Path,
    str::FromStr,
    sync::{Arc, atomic::AtomicI64},
    task::Poll,
    time::Duration,
};

use octocrab::Octocrab;
use rust_query::{DatabaseAsync, Transaction, aggregate};
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task, time::interval};

use crate::{
    database::{schema::Schema, updates::ProcessStatus},
    requests::{Priority, Request, limits::RequestLimits},
};

mod database;
mod requests;

pub use crate::database::schema;

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

pub struct GithubCredentials {
    pub app_id: String,
    pub app_secret: String,
}

pub struct GithubDb {
    db: DatabaseAsync<Schema>,
    octocrabs: Mutex<VecDeque<Arc<Octocrab>>>,

    limits: Mutex<RequestLimits>,
    request_sequence_number: AtomicI64,

    refresh: Mutex<tokio::time::Interval>,

    repos: Vec<Repo>,
}

impl GithubDb {
    pub async fn new(
        db_path: impl AsRef<Path>,
        credentials: &[GithubCredentials],
        requests_per_hour: usize,
        repos: &[&str],
    ) -> Self {
        let octocrabs = credentials
            .iter()
            .map(|GithubCredentials { app_id, app_secret }| {
                octocrab::Octocrab::builder()
                    .basic_auth(app_id.clone(), app_secret.clone())
                    .build()
                    .unwrap()
            })
            .map(Arc::new)
            .collect();
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
            octocrabs: Mutex::new(octocrabs),
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

    async fn octocrab(&self) -> Arc<Octocrab> {
        let mut octocrabs = self.octocrabs.lock().await;
        octocrabs.rotate_left(1);
        octocrabs.front().unwrap().clone()
    }

    pub async fn transaction<R: 'static + Send>(
        &self,
        f: impl 'static + Send + FnOnce(&'static Transaction<Schema>) -> R,
    ) -> R {
        self.db.transaction(f).await
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
                self.add_req(Priority::Index, oldpr).await;
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
                self.add_req(Priority::Index, oldissue).await;
            }

            self.add_req(
                Priority::Update,
                Request::NewPr {
                    repo: repo.clone(),
                    page: 0,
                    url: None,
                },
            )
            .await;
            self.add_req(
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
            self.add_req(
                Priority::Update,
                Request::NewPr {
                    repo: repo.clone(),
                    page: 0,
                    url: None,
                },
            )
            .await;
            self.add_req(
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
                        this.handle_request(r).await;
                    });
                    true
                } else {
                    tracing::debug!("no request for category {c:?}");
                    false
                }
            })
            .await;

        self.stats().await;
    }

    async fn stats(&self) {
        let (num_prs, num_issues, num_shared, num_users, num_comments, num_labels, num_requests) =
            self.db
                .transaction(move |txn| {
                    (
                        txn.query_one(aggregate(|row| {
                            use schema::*;
                            let r = row.join(PullRequest);
                            row.count_distinct(r)
                        })),
                        txn.query_one(aggregate(|row| {
                            use schema::*;
                            let r = row.join(Issue);
                            row.count_distinct(r)
                        })),
                        txn.query_one(aggregate(|row| {
                            use schema::*;
                            let r = row.join(IssuePullRequestShared);
                            row.count_distinct(r)
                        })),
                        txn.query_one(aggregate(|row| {
                            use schema::*;
                            let r = row.join(User);
                            row.count_distinct(r)
                        })),
                        txn.query_one(aggregate(|row| {
                            use schema::*;
                            let r = row.join(Comment);
                            row.count_distinct(r)
                        })),
                        txn.query_one(aggregate(|row| {
                            use schema::*;
                            let r = row.join(Label);
                            row.count_distinct(r)
                        })),
                        txn.query_one(aggregate(|row| {
                            use schema::*;
                            let r = row.join(Request);
                            row.count_distinct(r)
                        })),
                    )
                })
                .await;

        tracing::info!("{}", self.limits.lock().await);
        tracing::info!(
            "prs: {num_prs} issues: {num_issues} shared: {num_shared} users: {num_users} comments: {num_comments} labels: {num_labels} pending requests: {num_requests}"
        );
        let avg_time_btwn_req = self.limits.lock().await.average_time_between_requests();
        let req_per_hour = (3600 * 1000) / avg_time_btwn_req.as_millis().max(1);
        tracing::info!(
            "average time between requests: {avg_time_btwn_req:?} i.e. {req_per_hour} req/h"
        )
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
}

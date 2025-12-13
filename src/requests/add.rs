use std::sync::atomic::Ordering;

use crate::database::schema;
use crate::database::updates::ProcessStatus;
use crate::requests::{Priority, Request};
use crate::{GithubDb, Repo};

impl GithubDb {
    pub async fn add_req(&self, c: Priority, r: Request) {
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

    pub async fn add_comments_updated_req(
        &self,
        issue_status: ProcessStatus,
        repo: Repo,
        issue_updated_timestamp: Option<i64>,
        issue_number: u64,
    ) {
        let since = match issue_status {
            ProcessStatus::New => None,
            ProcessStatus::Updated => issue_updated_timestamp,
            ProcessStatus::Unchanged => return,
        };

        self.add_req(
            Priority::Comments,
            Request::Comments {
                repo,
                issue_number,
                since_timestamp: since,
                page: 0,
                url: None,
            },
        )
        .await;
    }
}

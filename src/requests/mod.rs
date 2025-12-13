use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::Repo;

pub mod add;
pub mod handle;
pub mod limits;

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

#[derive(Clone, Copy, Debug)]
pub enum Priority {
    // high prioriry, when things changed!
    Update = 0,
    // very low priority, tries to visit all items regularly
    Index,
    Comments,
}

impl Priority {
    const ALL: [Priority; 3] = [Self::Update, Self::Comments, Self::Index];

    fn fraction(&self) -> f64 {
        // must add to 1.0
        match self {
            Priority::Update => 0.3,
            Priority::Comments => 0.6,
            Priority::Index => 0.1,
        }
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
    Comments {
        repo: Repo,
        issue_number: u64,
        since_timestamp: Option<i64>,
        page: usize,
        url: Option<String>,
    },
}
impl Request {
    pub fn name(&self) -> &'static str {
        match self {
            Request::OldPr { .. } => "OldPr",
            Request::NewPr { .. } => "NewPr",
            Request::NewIssue { .. } => "NewIssue",
            Request::OldIssue { .. } => "OldIssue",
            Request::Comments { .. } => "Comments",
        }
    }
}

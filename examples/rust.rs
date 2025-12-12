use std::{env, sync::Arc, time::Duration};

use github_db::GithubDb;
use tracing::level_filters::LevelFilter;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_target(false)
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .with_level(true)
        .with_max_level(LevelFilter::DEBUG)
        .init();

    let gh = Arc::new(
        GithubDb::new(
            env::var("DB_PATH").unwrap(),
            env::var("GITHUB_APP_ID").unwrap(),
            env::var("GITHUB_APP_SECRET").unwrap(),
            2500,
            &["rust-lang/rust"],
        )
        .await,
    );

    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        interval.tick().await;
        gh.clone().update().await;
    }
}

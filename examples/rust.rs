use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use github_db::{GithubCredentials, GithubDb};
use tracing::level_filters::LevelFilter;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_target(false)
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .with_level(true)
        .with_max_level(LevelFilter::INFO)
        .init();

    let app_ids = env::var("GITHUB_APP_ID").unwrap();
    let app_secrets = env::var("GITHUB_APP_SECRET").unwrap();

    let credentials = app_ids
        .split(";;")
        .zip(app_secrets.split(";;"))
        .map(|(app_id, app_secret)| GithubCredentials {
            app_id: app_id.to_string(),
            app_secret: app_secret.to_string(),
        })
        .collect::<Vec<_>>();

    let gh = Arc::new(
        GithubDb::new(
            env::var("DB_PATH").unwrap(),
            &credentials,
            4000 * credentials.len(),
            &["rust-lang/rust"],
        )
        .await,
    );

    let done = Arc::new(AtomicBool::new(false));

    let mut interval = tokio::time::interval(Duration::from_secs(5));

    tokio::spawn({
        let done = done.clone();
        async move {
            tokio::signal::ctrl_c().await.unwrap();
            done.store(true, Ordering::Relaxed);
        }
    });

    while !done.load(Ordering::Relaxed) {
        interval.tick().await;
        gh.clone().update().await;
    }
}

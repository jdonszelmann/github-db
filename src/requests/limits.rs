use std::{
    fmt::Display,
    time::{Duration, Instant},
};

use crate::requests::Priority;

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
            category_limits: Priority::ALL
                .map(|i| (0.2 * limit as f64 * i.fraction(), Instant::now())),
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

            let limit = 0.2 * self.global_limit as f64 * category.fraction();
            if *before_count >= limit {
                saved_up = *before_count - limit;
                *before_count = limit;
            }
        }

        self.saved_up = saved_up.max(self.global_limit as f64 * 0.2);
    }
}

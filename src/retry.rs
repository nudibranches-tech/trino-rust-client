use std::time::Duration;

use backon::ExponentialBuilder;

/// Controls how the client retries **transient** failures (see
/// [`crate::client`] for which errors are considered retryable).
///
/// Retries use exponential backoff: the delay starts at `min_delay`, doubles
/// each attempt, and is capped at `max_delay`. `jitter` adds randomness to
/// spread retries and avoid a thundering herd.
///
/// The default matches the client's historical behaviour: 3 retries, 1s → 2s
/// backoff, no jitter.
///
/// ```
/// # use std::time::Duration;
/// # use trino_rust_client::retry::RetryPolicy;
/// let policy = RetryPolicy {
///     max_retries: 5,
///     min_delay: Duration::from_millis(200),
///     max_delay: Duration::from_secs(5),
///     jitter: true,
/// };
/// ```
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Maximum number of retries after the initial attempt.
    pub max_retries: usize,
    /// Delay before the first retry.
    pub min_delay: Duration,
    /// Upper bound on the backoff delay.
    pub max_delay: Duration,
    /// Add randomness to each delay to avoid synchronized retries.
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(2),
            jitter: false,
        }
    }
}

impl RetryPolicy {
    pub(crate) fn backoff(&self) -> ExponentialBuilder {
        let builder = ExponentialBuilder::default()
            .with_min_delay(self.min_delay)
            .with_max_delay(self.max_delay)
            .with_max_times(self.max_retries);
        if self.jitter {
            builder.with_jitter()
        } else {
            builder
        }
    }
}

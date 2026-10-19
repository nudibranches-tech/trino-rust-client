//! TLS backend plumbing.
//!
//! The TLS provider is selected with cargo features (see the crate-level
//! documentation). Everything here is a no-op unless `rustls-ring` is enabled:
//! that backend takes rustls without a built-in provider, so `ring` has to be
//! installed in the process before a `reqwest::Client` is built.

/// Install `ring` as the process-wide default rustls provider, unless one is
/// already installed.
#[cfg(feature = "rustls-ring")]
pub(crate) fn prepare_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        // Racing installs are fine: exactly one wins and the loser gets an
        // `Err` back, by which point a provider is installed either way.
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
}

/// Nothing to prepare: `rustls-aws-lc-rs` carries its own provider, and under a
/// bare `rustls-no-provider` build installing one is the caller's job.
#[cfg(not(feature = "rustls-ring"))]
pub(crate) fn prepare_crypto_provider() {}

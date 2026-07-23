use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde::Deserialize;

use crate::error::{Error, Result};

/// The redirect + token endpoints extracted from a Trino `WWW-Authenticate`
/// Bearer challenge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Challenge {
    pub x_redirect_server: String,
    pub x_token_server: String,
}

/// Parse a Trino OAuth2 `WWW-Authenticate: Bearer ...` challenge.
///
/// Returns `None` when the header is not a Bearer challenge or lacks
/// `x_token_server` (the one field the flow cannot proceed without).
pub fn parse_www_authenticate(header: &str) -> Option<Challenge> {
    let trimmed = header.trim();
    // Must be a Bearer challenge. Use `get` (not indexing) so a non-ASCII or
    // malformed header degrades to `None` instead of panicking on a byte slice
    // that lands inside a multi-byte char.
    match trimmed.get(..6) {
        Some(prefix) if prefix.eq_ignore_ascii_case("bearer") => {}
        _ => return None,
    }

    let mut x_redirect_server = None;
    let mut x_token_server = None;

    for part in trimmed.split(',') {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        // The first key may arrive as `bearer x_redirect_server`; take the last
        // whitespace-separated token as the real key.
        let key = key
            .trim()
            .rsplit(char::is_whitespace)
            .next()
            .unwrap_or("")
            .trim();
        let value = value.trim().trim_matches('"');
        match key.to_ascii_lowercase().as_str() {
            "x_redirect_server" => x_redirect_server = Some(value.to_string()),
            "x_token_server" => x_token_server = Some(value.to_string()),
            _ => {}
        }
    }

    Some(Challenge {
        // x_redirect_server can legitimately be absent (already-authenticated
        // reuse); default to empty so the handler simply has nothing to open.
        x_redirect_server: x_redirect_server.unwrap_or_default(),
        x_token_server: x_token_server?,
    })
}

/// Presents the OAuth2 login URL to the user. The client calls this once per
/// authentication; it must return promptly — it only *shows* the URL, it does
/// not wait for the user to finish (the client detects completion by polling).
pub trait RedirectHandler: Send + Sync {
    fn redirect(&self, url: &str) -> Result<()>;
}

/// Default handler: opens the system browser and also prints the URL to stderr
/// so headless / SSH sessions can still complete the flow.
pub struct BrowserRedirectHandler;

impl RedirectHandler for BrowserRedirectHandler {
    fn redirect(&self, url: &str) -> Result<()> {
        eprintln!("Open the following URL in a browser to authenticate:\n{url}");
        // Best-effort; failure to launch a browser is not fatal — the URL is
        // already on stderr.
        let _ = open::that(url);
        Ok(())
    }
}

/// Shared, interior-mutable state behind `Auth::OAuth2`. Cloning the enclosing
/// `Arc` shares one token cache across every clone of the `Client`.
pub struct OAuth2State {
    pub(crate) token: RwLock<Option<String>>,
    /// Serializes the browser+poll flow so concurrent 401s open one browser.
    // Read by the acquire/poll flow landing in a later task; unused until then.
    #[allow(dead_code)]
    pub(crate) acquire: tokio::sync::Mutex<()>,
    pub(crate) handler: Arc<dyn RedirectHandler>,
    pub(crate) max_poll_attempts: usize,
    pub(crate) poll_timeout: Duration,
}

impl OAuth2State {
    pub fn new(
        handler: Arc<dyn RedirectHandler>,
        max_poll_attempts: usize,
        poll_timeout: Duration,
    ) -> Self {
        Self {
            token: RwLock::new(None),
            acquire: tokio::sync::Mutex::new(()),
            handler,
            max_poll_attempts,
            poll_timeout,
        }
    }

    /// The currently cached bearer token, if the flow has completed.
    pub fn cached_token(&self) -> Option<String> {
        self.token.read().unwrap().clone()
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    token: Option<String>,
    #[serde(rename = "nextUri")]
    next_uri: Option<String>,
    error: Option<String>,
}

/// Present the login URL, then poll the token server following `nextUri` until a
/// token is returned, an error is reported, or attempts/timeout are exhausted.
// Called from `Client::send`'s 401 handling landing in a later task; unused
// outside tests until then.
#[allow(dead_code)]
pub(crate) async fn run_flow(
    client: &reqwest::Client,
    state: &OAuth2State,
    challenge: &Challenge,
) -> Result<String> {
    if !challenge.x_redirect_server.is_empty() {
        state.handler.redirect(&challenge.x_redirect_server)?;
    }

    let deadline = tokio::time::Instant::now() + state.poll_timeout;
    let mut url = challenge.x_token_server.clone();

    for _ in 0..state.max_poll_attempts {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let body = match tokio::time::timeout(remaining, async {
            let resp = client.get(&url).send().await?;
            resp.json::<TokenResponse>().await
        })
        .await
        {
            Ok(result) => result?, // network/decode error propagates as before
            Err(_elapsed) => break, // exceeded poll_timeout on this poll
        };

        if let Some(err) = body.error {
            return Err(Error::OAuth2(format!(
                "token endpoint returned error: {err}"
            )));
        }
        if let Some(token) = body.token {
            return Ok(token);
        }
        match body.next_uri {
            Some(next) => url = next,
            None => {
                return Err(Error::OAuth2(
                    "token endpoint response had neither token nor nextUri".to_string(),
                ))
            }
        }
    }

    Err(Error::OAuth2(format!(
        "authentication did not complete within {} attempts / {:?}",
        state.max_poll_attempts, state.poll_timeout
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_standard_challenge() {
        let h = r#"Bearer x_redirect_server="https://c/oauth2/token/initiate/abc", x_token_server="https://c/oauth2/token/abc""#;
        let c = parse_www_authenticate(h).expect("should parse");
        assert_eq!(c.x_redirect_server, "https://c/oauth2/token/initiate/abc");
        assert_eq!(c.x_token_server, "https://c/oauth2/token/abc");
    }

    #[test]
    fn tolerates_bearer_prefixed_key_quirk() {
        // Naive splitting yields the first key as `bearer x_redirect_server`.
        let h = r#"Bearer x_redirect_server="https://c/i", x_token_server="https://c/t""#;
        let c = parse_www_authenticate(h).expect("should parse");
        assert_eq!(c.x_redirect_server, "https://c/i");
        assert_eq!(c.x_token_server, "https://c/t");
    }

    #[test]
    fn ignores_param_order_and_extra_params() {
        let h = r#"Bearer realm="trino", x_token_server="https://c/t", x_redirect_server="https://c/i""#;
        let c = parse_www_authenticate(h).expect("should parse");
        assert_eq!(c.x_token_server, "https://c/t");
        assert_eq!(c.x_redirect_server, "https://c/i");
    }

    #[test]
    fn none_when_no_token_server() {
        let h = r#"Bearer x_redirect_server="https://c/i""#;
        assert!(parse_www_authenticate(h).is_none());
    }

    #[test]
    fn none_when_not_bearer() {
        assert!(parse_www_authenticate(r#"Basic realm="trino""#).is_none());
    }

    #[test]
    fn none_on_non_ascii_header_without_panicking() {
        // A multi-byte char straddling byte index 6 must not panic the byte slice.
        assert!(parse_www_authenticate("aaaaaé x_token_server=\"https://c/t\"").is_none());
    }

    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn state_defaults_have_no_token() {
        let state = OAuth2State::new(
            Arc::new(BrowserRedirectHandler),
            10,
            Duration::from_secs(120),
        );
        assert!(state.cached_token().is_none());
    }

    #[test]
    fn state_stores_and_reads_token() {
        let state = OAuth2State::new(
            Arc::new(BrowserRedirectHandler),
            10,
            Duration::from_secs(120),
        );
        *state.token.write().unwrap() = Some("tok".to_string());
        assert_eq!(state.cached_token().as_deref(), Some("tok"));
    }

    struct RecordingHandler {
        seen: std::sync::Mutex<Vec<String>>,
    }
    impl RedirectHandler for RecordingHandler {
        fn redirect(&self, url: &str) -> Result<()> {
            self.seen.lock().unwrap().push(url.to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn run_flow_follows_next_uri_then_returns_token() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // First poll -> keep polling (nextUri to /token/step2).
        Mock::given(method("GET"))
            .and(path("/token/step1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "nextUri": format!("{}/token/step2", server.uri())
            })))
            .mount(&server)
            .await;
        // Second poll -> token ready.
        Mock::given(method("GET"))
            .and(path("/token/step2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "token": "final-token"
            })))
            .mount(&server)
            .await;

        let handler = Arc::new(RecordingHandler {
            seen: std::sync::Mutex::new(vec![]),
        });
        let state = OAuth2State::new(handler.clone(), 10, Duration::from_secs(30));
        let challenge = Challenge {
            x_redirect_server: "https://login.example/redirect".to_string(),
            x_token_server: format!("{}/token/step1", server.uri()),
        };

        let token = run_flow(&reqwest::Client::new(), &state, &challenge)
            .await
            .expect("flow should succeed");

        assert_eq!(token, "final-token");
        assert_eq!(
            handler.seen.lock().unwrap().as_slice(),
            &["https://login.example/redirect".to_string()]
        );
    }

    #[tokio::test]
    async fn run_flow_surfaces_error_field() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/token/err"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "error": "access_denied"
            })))
            .mount(&server)
            .await;

        let state = OAuth2State::new(
            Arc::new(BrowserRedirectHandler),
            10,
            Duration::from_secs(30),
        );
        let challenge = Challenge {
            x_redirect_server: String::new(),
            x_token_server: format!("{}/token/err", server.uri()),
        };

        let err = run_flow(&reqwest::Client::new(), &state, &challenge)
            .await
            .unwrap_err();
        match err {
            crate::error::Error::OAuth2(msg) => assert!(msg.contains("access_denied")),
            other => panic!("expected OAuth2 error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn run_flow_bounded_by_poll_timeout_when_server_stalls() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/token/stall"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(30))
                    .set_body_json(serde_json::json!({ "token": "never-arrives-in-time" })),
            )
            .mount(&server)
            .await;

        let state = OAuth2State::new(
            Arc::new(BrowserRedirectHandler),
            10,
            Duration::from_millis(150),
        );
        let challenge = Challenge {
            x_redirect_server: String::new(),
            x_token_server: format!("{}/token/stall", server.uri()),
        };

        let err = run_flow(&reqwest::Client::new(), &state, &challenge)
            .await
            .unwrap_err();
        match err {
            crate::error::Error::OAuth2(msg) => assert!(msg.contains("did not complete")),
            other => panic!("expected OAuth2 timeout error, got {other:?}"),
        }
    }
}

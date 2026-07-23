use std::sync::{Arc, Mutex};
use std::time::Duration;

use trino_rust_client::auth::{Auth, RedirectHandler};
use trino_rust_client::client::ClientBuilder;
use trino_rust_client::error::Result as TrinoResult;
use trino_rust_client::{Client, Row};

use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct RecordingHandler {
    seen: Mutex<Vec<String>>,
}
impl RedirectHandler for RecordingHandler {
    fn redirect(&self, url: &str) -> TrinoResult<()> {
        self.seen.lock().unwrap().push(url.to_string());
        Ok(())
    }
}

/// wiremock 0.6 has no built-in "header absent" combinator.
struct HeaderAbsent(&'static str);
impl wiremock::Match for HeaderAbsent {
    fn matches(&self, req: &wiremock::Request) -> bool {
        !req.headers.contains_key(self.0)
    }
}

fn client_for(server: &MockServer, handler: Arc<dyn RedirectHandler>) -> Client {
    let host = server.uri().replace("http://", "");
    let (host, port) = host.split_once(':').unwrap();
    ClientBuilder::new("test-user", host)
        .port(port.parse().unwrap())
        .secure(false)
        .auth_http_insecure(true)
        .auth(Auth::new_oauth2_with_handler(handler))
        .build()
        .unwrap()
}

/// Minimal terminal (FINISHED, no nextUri) statement response, reusing the
/// exact field names from `tests/data/models/query_result_empty`.
fn finished_query_json() -> String {
    std::fs::read_to_string("tests/data/models/query_result_empty").unwrap()
}

#[tokio::test]
async fn oauth2_happy_path_authenticates_and_caches() {
    let server = MockServer::start().await;
    let challenge = format!(
        r#"Bearer x_redirect_server="https://login/redirect", x_token_server="{}/oauth2/token/abc""#,
        server.uri()
    );

    // First statement submission with no token -> 401 with challenge.
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .and(HeaderAbsent("authorization"))
        .respond_with(
            ResponseTemplate::new(401).insert_header("WWW-Authenticate", challenge.as_str()),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Token endpoint -> token immediately.
    Mock::given(method("GET"))
        .and(path("/oauth2/token/abc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "token": "test-token"
        })))
        .mount(&server)
        .await;

    // Authenticated submission -> a terminal QueryResult fixture.
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(finished_query_json()))
        .mount(&server)
        .await;

    let handler = Arc::new(RecordingHandler {
        seen: Mutex::new(vec![]),
    });
    let client = client_for(&server, handler.clone());

    client.get_all::<Row>("SELECT 1").await.expect("query ok");
    // Browser presented exactly once.
    assert_eq!(handler.seen.lock().unwrap().len(), 1);

    // Second query reuses the cached token: no new 401 mock is needed, and the
    // handler is not called again.
    client.get_all::<Row>("SELECT 2").await.expect("query ok");
    assert_eq!(handler.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn oauth2_single_flight_opens_browser_once() {
    let server = MockServer::start().await;
    // Include x_redirect_server (unlike a bare-minimum challenge) so the
    // handler is actually invoked and the "opens once" assertion is meaningful.
    let challenge = format!(
        r#"Bearer x_redirect_server="https://login/redirect", x_token_server="{}/oauth2/token/abc""#,
        server.uri()
    );
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .and(HeaderAbsent("authorization"))
        .respond_with(
            ResponseTemplate::new(401).insert_header("WWW-Authenticate", challenge.as_str()),
        )
        .mount(&server)
        .await;
    // Token endpoint is slow enough that both requests race into the flow.
    Mock::given(method("GET"))
        .and(path("/oauth2/token/abc"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(200))
                .set_body_json(serde_json::json!({ "token": "test-token" })),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(finished_query_json()))
        .mount(&server)
        .await;

    let handler = Arc::new(RecordingHandler {
        seen: Mutex::new(vec![]),
    });
    let client = Arc::new(client_for(&server, handler.clone()));

    let c1 = client.clone();
    let c2 = client.clone();
    let (r1, r2) = tokio::join!(c1.get_all::<Row>("SELECT 1"), c2.get_all::<Row>("SELECT 2"),);
    r1.expect("q1 ok");
    r2.expect("q2 ok");

    // Both queries authenticated, but the browser was presented once.
    assert_eq!(handler.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn oauth2_401_without_challenge_is_http_not_ok() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(401)) // no WWW-Authenticate
        .mount(&server)
        .await;

    let handler = Arc::new(RecordingHandler {
        seen: Mutex::new(vec![]),
    });
    let client = client_for(&server, handler.clone());

    let err = client.get_all::<Row>("SELECT 1").await.unwrap_err();
    assert!(matches!(
        err,
        trino_rust_client::error::Error::HttpNotOk(code, _) if code == reqwest::StatusCode::UNAUTHORIZED
    ));
    assert_eq!(handler.seen.lock().unwrap().len(), 0);
}

/// End-to-end against a real Trino coordinator configured for OAuth2. Not run
/// in CI — there is no committed docker-compose stack for this; point it at
/// your own Trino + IdP setup (see "Manual OAuth2 e2e" in `CLAUDE.md`).
///
/// Run locally:
///   TRINO_OAUTH2_HOST=coordinator.example.com cargo test --test oauth2 -- --ignored oauth2_real_login
/// A browser window opens for the IdP login; complete it to let the test pass.
#[ignore = "requires a real OAuth2-configured Trino coordinator and an interactive browser login"]
#[tokio::test]
async fn oauth2_real_login() {
    let host = std::env::var("TRINO_OAUTH2_HOST").expect("set TRINO_OAUTH2_HOST");
    let client = ClientBuilder::new("test-user", host)
        .secure(true)
        .auth(Auth::new_oauth2())
        .build()
        .unwrap();
    let ds = client
        .get_all::<Row>("SELECT 1")
        .await
        .expect("query ok after login");
    assert_eq!(ds.len(), 1);
}

use std::sync::{Arc, Mutex};
use std::time::Duration;

use trino_rust_client::auth::{Auth, RedirectHandler};
use trino_rust_client::client::ClientBuilder;
use trino_rust_client::error::Result as TrinoResult;
use trino_rust_client::{Client, Row};

use wiremock::matchers::{body_string_contains, header, method, path};
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

/// Matches iff the request carries EXACTLY ONE `Authorization` header whose
/// value is `Bearer <token>`. Guards against `bearer_auth` appending a second
/// header on the OAuth2 re-auth retry.
struct SingleBearer(&'static str);
impl wiremock::Match for SingleBearer {
    fn matches(&self, req: &wiremock::Request) -> bool {
        let vals: Vec<_> = req
            .headers
            .get_all(reqwest::header::AUTHORIZATION)
            .into_iter()
            .collect();
        vals.len() == 1 && vals[0].to_str().ok() == Some(&format!("Bearer {}", self.0))
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

/// Simulates a cached OAuth2 token expiring between two queries on the same
/// client. The re-auth retry must carry exactly ONE `Authorization` header —
/// `bearer_auth` APPENDS, so if auth is applied before the retry clone is taken
/// the retried request goes out with two headers and the coordinator rejects it.
#[tokio::test]
async fn oauth2_reauth_on_expiry_sends_single_authorization_header() {
    let server = MockServer::start().await;

    let challenge1 = format!(
        r#"Bearer x_redirect_server="https://login/redirect", x_token_server="{}/oauth2/token/1""#,
        server.uri()
    );
    let challenge2 = format!(
        r#"Bearer x_redirect_server="https://login/redirect", x_token_server="{}/oauth2/token/2""#,
        server.uri()
    );

    // --- Query 1: no cached token -> 401 challenge -> acquire token-v1 -> 200.
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .and(body_string_contains("SELECT 1"))
        .and(HeaderAbsent("authorization"))
        .respond_with(
            ResponseTemplate::new(401).insert_header("WWW-Authenticate", challenge1.as_str()),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/oauth2/token/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "token": "token-v1"
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .and(body_string_contains("SELECT 1"))
        .and(header("authorization", "Bearer token-v1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(finished_query_json()))
        .mount(&server)
        .await;

    // --- Query 2: cache attaches (now-expired) token-v1 -> coordinator 401s ->
    // acquire token-v2 -> retry must send a SINGLE Bearer token-v2 header.
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .and(body_string_contains("SELECT 2"))
        .and(header("authorization", "Bearer token-v1"))
        .respond_with(
            ResponseTemplate::new(401).insert_header("WWW-Authenticate", challenge2.as_str()),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/oauth2/token/2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "token": "token-v2"
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .and(body_string_contains("SELECT 2"))
        .and(SingleBearer("token-v2"))
        .respond_with(ResponseTemplate::new(200).set_body_string(finished_query_json()))
        .mount(&server)
        .await;

    let handler = Arc::new(RecordingHandler {
        seen: Mutex::new(vec![]),
    });
    let client = client_for(&server, handler.clone());

    client.get_all::<Row>("SELECT 1").await.expect("query 1 ok");
    client.get_all::<Row>("SELECT 2").await.expect("query 2 ok");
}

/// End-to-end against a real Trino coordinator configured for OAuth2. Not run
/// in CI — the interactive flow needs a human to complete the browser login.
///
/// Run against the bundled local stack (Trino + Keycloak) at
/// `integration_tests/test_setup/oauth/` (see its README for the one-time
/// `/etc/hosts` step and the two setup gotchas):
///
///   docker compose -f integration_tests/test_setup/oauth/docker-compose.yml up -d
///   TRINO_OAUTH2_HOST=localhost TRINO_OAUTH2_PORT=8443 TRINO_OAUTH2_NO_VERIFY=1 \
///       cargo test --test oauth2 -- --ignored oauth2_real_login
///
/// A browser opens for the Keycloak login (user `alice` / `alice`); complete it
/// to let the test pass. Point it at your own coordinator by setting just
/// `TRINO_OAUTH2_HOST` (and `TRINO_OAUTH2_PORT` if not 443).
#[ignore = "requires a real OAuth2-configured Trino coordinator and an interactive browser login"]
#[tokio::test]
async fn oauth2_real_login() {
    let host = std::env::var("TRINO_OAUTH2_HOST").expect("set TRINO_OAUTH2_HOST");
    // The Trino session user must match the authenticated OAuth2 principal
    // (Keycloak `preferred_username`), otherwise Trino rejects the query as
    // impersonation. The bundled stack's user is `alice`.
    let user = std::env::var("TRINO_OAUTH2_USER").unwrap_or_else(|_| "alice".to_string());
    let mut builder = ClientBuilder::new(user, host)
        .secure(true)
        .auth(Auth::new_oauth2());
    if let Ok(port) = std::env::var("TRINO_OAUTH2_PORT") {
        builder = builder.port(port.parse().expect("TRINO_OAUTH2_PORT must be a u16"));
    }
    // The bundled local stack uses a self-signed certificate; set
    // TRINO_OAUTH2_NO_VERIFY=1 to skip TLS verification against it.
    if std::env::var("TRINO_OAUTH2_NO_VERIFY").is_ok() {
        builder = builder.no_verify(true);
    }
    let client = builder.build().unwrap();
    let ds = client
        .get_all::<Row>("SELECT 1")
        .await
        .expect("query ok after login");
    assert_eq!(ds.len(), 1);
}

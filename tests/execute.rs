//! Regression tests: `execute` used to fetch its final result page with a bare
//! GET that skipped the client's request pipeline, and with it the auth
//! headers and the response status check.

// Every test here builds a client, which needs a rustls crypto provider — a
// bring-your-own-provider build (`rustls-no-provider` without `rustls-ring`)
// has none.
#![cfg(any(feature = "rustls-aws-lc-rs", feature = "rustls-ring"))]

use trino_rust_client::auth::Auth;
use trino_rust_client::client::ClientBuilder;
use trino_rust_client::error::Error;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

/// `alice:alice`, as `reqwest` encodes it.
const BASIC_AUTH: &str = "Basic YWxpY2U6YWxpY2U=";

async fn make_mock_server() -> (MockServer, String, u16) {
    let server = MockServer::start().await;
    let uri = server.uri();
    let host_port = uri.trim_start_matches("http://");
    let (host, port_str) = host_port.rsplit_once(':').unwrap();
    let port: u16 = port_str.parse().unwrap();
    (server, host.to_string(), port)
}

/// `next` is the path to follow, or `None` for the final page.
fn page(server_uri: &str, id: &str, next: Option<&str>) -> String {
    let next_uri = match next {
        Some(p) => format!(r#""nextUri": "{server_uri}{p}","#),
        None => String::new(),
    };
    format!(
        r#"{{
            "id": "{id}",
            "infoUri": "{server_uri}/ui/query.html?{id}",
            {next_uri}
            "stats": {{
                "state": "FINISHED", "queued": false, "scheduled": false,
                "nodes": 0, "totalSplits": 0, "queuedSplits": 0,
                "runningSplits": 0, "completedSplits": 0,
                "cpuTimeMillis": 0, "wallTimeMillis": 0, "queuedTimeMillis": 0,
                "elapsedTimeMillis": 0, "processedRows": 0, "processedBytes": 0,
                "peakMemoryBytes": 0, "spilledBytes": 0
            }},
            "warnings": [],
            "updateType": "CREATE TABLE"
        }}"#
    )
}

fn header_value(req: &Request, name: &str) -> Option<String> {
    req.headers
        .get(name)
        .map(|v| v.to_str().unwrap().to_string())
}

async fn mount_authenticated_statement_mocks(server: &MockServer) {
    let uri = server.uri();

    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .and(header("Authorization", BASIC_AUTH))
        .respond_with(ResponseTemplate::new(200).set_body_string(page(
            &uri,
            "test_execute_00001",
            Some("/v1/statement/test_execute_00001/1"),
        )))
        .expect(1)
        .with_priority(1)
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/statement/test_execute_00001/1"))
        .and(header("Authorization", BASIC_AUTH))
        .respond_with(ResponseTemplate::new(200).set_body_string(page(
            &uri,
            "test_execute_00001",
            None,
        )))
        .expect(1)
        .with_priority(1)
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(401)
                .insert_header("WWW-Authenticate", r#"Basic realm="Trino""#)
                .set_body_string("<html><body>Unauthorized</body></html>"),
        )
        .expect(0)
        .with_priority(10)
        .mount(server)
        .await;
}

#[tokio::test]
async fn test_execute_authenticates_every_request() {
    let (server, host, port) = make_mock_server().await;
    mount_authenticated_statement_mocks(&server).await;

    let client = ClientBuilder::new("alice", host)
        .port(port)
        .auth(Auth::new_basic("alice", Some("alice")))
        // Lifts the "no basic auth over http" guard, for the mock server.
        .auth_http_insecure(true)
        .build()
        .unwrap();

    let result = client
        .execute("CREATE TABLE IF NOT EXISTS t (a integer)")
        .await;

    assert!(
        result.is_ok(),
        "every request execute makes must go through the authenticated path, got: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap().update_type.as_deref(), Some("CREATE TABLE"));

    for req in server.received_requests().await.unwrap() {
        let at = format!("{} {}", req.method, req.url.path());
        assert_eq!(
            header_value(&req, "Authorization").as_deref(),
            Some(BASIC_AUTH),
            "{at} went out unauthenticated"
        );
        assert_eq!(
            header_value(&req, "X-Trino-User").as_deref(),
            Some("alice"),
            "{at} is missing X-Trino-User"
        );
        assert_eq!(
            header_value(&req, "User-Agent").as_deref(),
            Some("trino-rust-client"),
            "{at} is missing the client's User-Agent"
        );
    }

    // Each page fetched exactly once: no second fetch of the final page.
    server.verify().await;
}

/// A rejected fetch used to be deserialized without a status check, surfacing
/// as `HttpError(Decode)` rather than as the status Trino returned.
#[tokio::test]
async fn test_execute_reports_a_rejected_page_fetch_as_http_not_ok() {
    let (server, host, port) = make_mock_server().await;
    let uri = server.uri();

    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_string(page(
            &uri,
            "test_execute_00002",
            Some("/v1/statement/test_execute_00002/1"),
        )))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/statement/test_execute_00002/1"))
        .respond_with(
            ResponseTemplate::new(401)
                .insert_header("WWW-Authenticate", r#"Basic realm="Trino""#)
                .set_body_string("<html><body>Unauthorized</body></html>"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let err = ClientBuilder::new("alice", host)
        .port(port)
        .build()
        .unwrap()
        .execute("CREATE TABLE IF NOT EXISTS t (a integer)")
        .await
        .expect_err("a 401 must not be reported as success");

    match err {
        Error::HttpNotOk(status, body) => {
            assert_eq!(status, 401);
            assert!(body.contains("Unauthorized"), "body was: {body}");
        }
        other => panic!("expected Error::HttpNotOk, got: {other:?}"),
    }

    server.verify().await;
}

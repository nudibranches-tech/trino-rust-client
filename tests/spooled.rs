use std::fs;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use trino_rust_client::{ClientBuilder, Row, Trino};

#[tokio::test]
async fn test_spooled_inline() {
    let body = fs::read_to_string("tests/data/models/query_result_spooled_inline").unwrap();

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/statement"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(&server)
        .await;

    let client = ClientBuilder::new("user", "127.0.0.1")
        .port(server.address().port())
        .query_data_encoding("json")
        .build()
        .unwrap();

    let data = client.get_all::<Row>("select 1".to_string()).await.unwrap();
    let rows = data.into_vec();
    assert_eq!(rows.len(), 2);
}

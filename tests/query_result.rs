#![allow(incomplete_features)]

use std::fs::File;
use std::io::Read;

use serde::Deserialize;
use trino_rust_client::models::QueryResultData;
use trino_rust_client::{QueryResult, Row, Trino};

fn read(name: &str) -> String {
    let p = "tests/data/models/".to_string() + name;
    let mut f = File::open(p).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    buf
}

#[derive(Trino, PartialEq, Debug, Clone, Deserialize)]
struct A {
    a: String,
    b: i32,
    c: bool,
    d: Vec<i32>,
    e: B,
    f: Option<String>,
}

#[derive(Trino, PartialEq, Debug, Clone, Deserialize)]
struct B {
    x: i64,
    y: f64,
}

#[test]
fn test_queued() {
    let s = read("query_result_queued");
    let d = serde_json::from_str::<QueryResult<A>>(&s).unwrap();
    let uri = "http://localhost:11032/v1/statement/20200513_074020_00002_mgdh8/x26d7c0451ed24f5fb3d68cb79e6bdad2/1";

    assert_eq!(d.next_uri, Some(uri.into()));
    assert!(d.data.is_none());
    assert!(d.error.is_none());
}

#[test]
fn test_planning() {
    let s = read("query_result_planning");
    let d = serde_json::from_str::<QueryResult<A>>(&s).unwrap();
    let uri = "http://localhost:11032/v1/statement/20200514_063813_02434_mgdh8/xf7e62a5d1e1a4bd49f9341379c477ed1/2";

    assert_eq!(d.next_uri, Some(uri.into()));
    assert!(d.data.is_none());
    assert!(d.error.is_none());
}

#[test]
fn test_running() {
    let s = read("query_result_running");
    let d = serde_json::from_str::<QueryResult<Row>>(&s).unwrap();

    assert!(d.error.is_none());

    // Extract data from Direct variant
    match d.data.unwrap() {
        QueryResultData::Direct(rows) => {
            assert_eq!(rows.len(), 1);
        }
        _ => panic!("Expected Direct variant"),
    }
}

#[test]
fn test_finished() {
    let s = read("query_result_finished");
    let d = serde_json::from_str::<QueryResult<A>>(&s).unwrap();

    assert!(d.next_uri.is_none());
    assert!(d.data.is_some());
    assert!(d.error.is_none());
}

#[test]
fn test_finished_prepare() {
    let s = read("query_result_finished_prepare");
    let d = serde_json::from_str::<QueryResult<A>>(&s).unwrap();

    assert!(d.next_uri.is_none());
    assert!(d.data.is_none());
    assert!(d.error.is_none());
    assert_eq!(d.update_type, Some("PREPARE".to_string()));
}

#[test]
fn test_failed() {
    let s = read("query_result_failed");
    let d = serde_json::from_str::<QueryResult<A>>(&s).unwrap();

    assert!(d.next_uri.is_none());
    assert!(d.data.is_none());
    assert!(d.error.is_some());
}

#[test]
fn test_empty() {
    let s = read("query_result_empty");
    let d = serde_json::from_str::<QueryResult<A>>(&s).unwrap();

    assert!(d.next_uri.is_none());
    assert!(d.columns.is_some());
    assert!(d.data.is_none());
    assert!(d.error.is_none());
}

//! Round-trip of the Trino transaction protocol against a real coordinator.
//!
//! Transaction identifiers come from Trino's transaction manager rather than
//! from a connector, so the `memory` catalog exercises the whole protocol.

use trino_integration_tests::set_test_fixture;
use trino_rust_client::transaction::TransactionId;
use trino_rust_client::{ClientBuilder, Row};

#[tokio::test]
async fn transaction_round_trip() {
    let fixture = set_test_fixture("transaction_round_trip");

    let client = ClientBuilder::new("test", &fixture.coordinator_host)
        .port(fixture.coordinator_port)
        .catalog("memory")
        .schema("default")
        .build()
        .expect("Failed to create client");

    assert_eq!(
        client.transaction_id().await,
        TransactionId::NoTransaction,
        "a fresh client must not be in a transaction"
    );

    client
        .begin_transaction()
        .await
        .expect("START TRANSACTION failed");

    let started = client.transaction_id().await;
    assert!(
        started.is_active(),
        "Trino returned a transaction id but the client dropped it: {started:?}"
    );

    // A statement inside the transaction must succeed. This does not prove it
    // ran transactionally-scoped: Trino answers this query whether or not the
    // transaction header is set, and committing an empty transaction succeeds
    // either way. The wire-level proof that the identifier is actually sent
    // lives in tests/transaction.rs; what this asserts is that a real
    // coordinator issues an identifier, holds it across a statement, and
    // clears it on COMMIT.
    let rows = client
        .get_all::<Row>("SELECT 1")
        .await
        .expect("SELECT inside transaction failed");
    assert_eq!(rows.len(), 1);

    assert_eq!(
        client.transaction_id().await,
        started,
        "the transaction id must not change mid-transaction"
    );

    client.commit().await.expect("COMMIT failed");

    assert_eq!(
        client.transaction_id().await,
        TransactionId::NoTransaction,
        "COMMIT must clear the transaction id"
    );
}

#[tokio::test]
async fn rollback_ends_the_transaction() {
    let fixture = set_test_fixture("rollback_ends_the_transaction");

    let client = ClientBuilder::new("test", &fixture.coordinator_host)
        .port(fixture.coordinator_port)
        .catalog("memory")
        .schema("default")
        .build()
        .expect("Failed to create client");

    client
        .begin_transaction()
        .await
        .expect("START TRANSACTION failed");
    assert!(client.transaction_id().await.is_active());

    client.rollback().await.expect("ROLLBACK failed");
    assert_eq!(
        client.transaction_id().await,
        TransactionId::NoTransaction,
        "ROLLBACK must clear the transaction id"
    );
}

/// Trino rejects a second `START TRANSACTION`; the client rejects it locally,
/// without a round trip.
#[tokio::test]
async fn nested_transactions_are_rejected() {
    let fixture = set_test_fixture("nested_transactions_are_rejected");

    let client = ClientBuilder::new("test", &fixture.coordinator_host)
        .port(fixture.coordinator_port)
        .catalog("memory")
        .schema("default")
        .build()
        .expect("Failed to create client");

    client
        .begin_transaction()
        .await
        .expect("START TRANSACTION failed");

    let err = client
        .begin_transaction()
        .await
        .expect_err("nested transaction must be rejected");
    assert!(
        matches!(err, trino_rust_client::error::Error::Transaction(_)),
        "expected Error::Transaction, got {err:?}"
    );

    client.rollback().await.expect("ROLLBACK failed");
}

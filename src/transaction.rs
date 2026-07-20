/// The transaction a session is bound to.
///
/// This models the `X-Trino-Transaction-Id` request header, which carries
/// either the `NONE` sentinel or a transaction identifier issued by Trino.
///
/// Trino returns the identifier of a newly started transaction in the
/// `X-Trino-Started-Transaction-Id` response header; the client captures it
/// automatically, so callers normally reach for
/// [`Client::begin_transaction`](crate::client::Client::begin_transaction)
/// rather than constructing this type by hand.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum TransactionId {
    /// No transaction is active; requests send `NONE`.
    #[default]
    NoTransaction,
    /// A transaction identifier issued by Trino.
    Id(String),
}

impl TransactionId {
    /// The value to send in the `X-Trino-Transaction-Id` request header.
    pub fn as_header_value(&self) -> &str {
        match self {
            Self::NoTransaction => "NONE",
            Self::Id(id) => id,
        }
    }

    /// Parse a value received from Trino.
    ///
    /// Infallible: anything that is not the `NONE` sentinel is a transaction
    /// identifier. Trino does not document a fixed format for it, so no
    /// validation is applied.
    pub fn from_header_value(s: &str) -> Self {
        match s {
            "NONE" => Self::NoTransaction,
            other => Self::Id(other.to_string()),
        }
    }

    /// Whether a transaction is currently active.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Id(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bug this crate shipped in 0.11.0: a real UUID from
    /// `X-Trino-Started-Transaction-Id` parsed to `None` and was discarded.
    #[test]
    fn uuid_is_retained_as_transaction_id() {
        let uuid = "17cbc429-462a-4da3-9a06-02b6507d0d01";
        assert_eq!(
            TransactionId::from_header_value(uuid),
            TransactionId::Id(uuid.to_string())
        );
    }

    #[test]
    fn none_sentinel_maps_to_no_transaction() {
        assert_eq!(
            TransactionId::from_header_value("NONE"),
            TransactionId::NoTransaction
        );
    }

    #[test]
    fn header_value_round_trips() {
        let uuid = "17cbc429-462a-4da3-9a06-02b6507d0d01";
        let id = TransactionId::from_header_value(uuid);
        assert_eq!(id.as_header_value(), uuid);
        assert_eq!(TransactionId::from_header_value(id.as_header_value()), id);
    }

    #[test]
    fn default_is_no_transaction_and_serialises_to_none() {
        let id = TransactionId::default();
        assert_eq!(id, TransactionId::NoTransaction);
        assert_eq!(id.as_header_value(), "NONE");
        assert!(!id.is_active());
    }

    #[test]
    fn only_id_is_active() {
        assert!(TransactionId::Id("abc".to_string()).is_active());
        assert!(!TransactionId::NoTransaction.is_active());
    }
}

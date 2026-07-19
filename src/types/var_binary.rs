use base64::Engine;
use serde::de::{Deserialize, DeserializeSeed, Deserializer};
use serde::{Serialize, Serializer};

use super::{Context, Trino, TrinoTy};

/// A Trino `VARBINARY` value — raw bytes.
///
/// Trino encodes binary values as base64 strings on the wire; this type decodes
/// and encodes that transparently, exposing the decoded bytes as `.0`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VarBinary(pub Vec<u8>);

impl Serialize for VarBinary {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(&self.0))
    }
}

impl<'de> Deserialize<'de> for VarBinary {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)?;
        Ok(VarBinary(bytes))
    }
}

impl Trino for VarBinary {
    type ValueType<'a> = String;
    type Seed<'a, 'de> = VarBinarySeed;

    fn value(&self) -> Self::ValueType<'_> {
        base64::engine::general_purpose::STANDARD.encode(&self.0)
    }

    fn ty() -> TrinoTy {
        TrinoTy::VarBinary
    }

    fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
        VarBinarySeed
    }

    fn empty() -> Self {
        VarBinary(Vec::new())
    }
}

pub struct VarBinarySeed;

impl<'de> DeserializeSeed<'de> for VarBinarySeed {
    type Value = VarBinary;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        VarBinary::deserialize(deserializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_trino_base64() {
        // Trino sends VARBINARY as a base64 string; "aGVsbG8=" is "hello".
        let v: VarBinary = serde_json::from_str("\"aGVsbG8=\"").unwrap();
        assert_eq!(v.0, b"hello");
    }

    #[test]
    fn round_trips_through_base64() {
        let original = VarBinary(vec![0, 1, 2, 253, 254, 255]);
        let json = serde_json::to_string(&original).unwrap();
        let back: VarBinary = serde_json::from_str(&json).unwrap();
        assert_eq!(original, back);
        // `Trino::value` emits the same base64 the wire uses.
        assert_eq!(json, format!("\"{}\"", original.value()));
    }
}

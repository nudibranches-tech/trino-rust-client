use serde::de::{Deserialize, DeserializeSeed, Deserializer};

use super::{Context, Trino, TrinoMapKey, TrinoTy};

impl Trino for bool {
    type ValueType<'a> = &'a bool;
    type Seed<'a, 'de> = BoolSeed;

    fn value(&self) -> Self::ValueType<'_> {
        self
    }

    fn ty() -> TrinoTy {
        TrinoTy::Boolean
    }

    fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
        BoolSeed
    }

    fn empty() -> Self {
        Default::default()
    }
}

impl TrinoMapKey for bool {}

pub struct BoolSeed;

impl<'de> DeserializeSeed<'de> for BoolSeed {
    type Value = bool;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::Value::deserialize(deserializer)
    }
}

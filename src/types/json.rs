use crate::{Context, Trino, TrinoTy};
use serde::de::DeserializeSeed;
use serde::{Deserialize, Deserializer};
use serde_json::Value;

impl Trino for Value {
    type ValueType<'a> = &'a Value;
    type Seed<'a, 'de> = ValueSeed;

    fn value(&self) -> Self::ValueType<'_> {
        self
    }

    fn ty() -> TrinoTy {
        TrinoTy::Json
    }

    fn seed<'a, 'de>(_: &'a Context<'a>) -> Self::Seed<'a, 'de> {
        ValueSeed
    }

    fn empty() -> Self {
        Default::default()
    }
}

pub struct ValueSeed;

impl<'de> DeserializeSeed<'de> for ValueSeed {
    type Value = Value;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        <Value as Deserialize<'de>>::deserialize(deserializer)
    }
}

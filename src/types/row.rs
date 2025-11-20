use serde::de::{Deserialize, DeserializeSeed, Deserializer};
use serde::Serialize;
use serde_json::Value;

use crate::{Context, Trino, TrinoTy};

#[derive(Debug, Clone, Serialize)]
pub struct Row {
    data: Vec<Value>,
}

impl Row {
    pub fn into_json(self) -> Vec<Value> {
        self.data
    }
}

impl Trino for Row {
    type ValueType<'a> = &'a [Value];
    type Seed<'a, 'de> = RowSeed;

    fn value(&self) -> Self::ValueType<'_> {
        &self.data
    }

    fn ty() -> TrinoTy {
        TrinoTy::Unknown
    }

    fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
        RowSeed
    }

    fn empty() -> Self {
        Row { data: vec![] }
    }
}

pub struct RowSeed;

impl<'de> DeserializeSeed<'de> for RowSeed {
    type Value = Row;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = <Vec<Value>>::deserialize(deserializer)?;
        Ok(Row { data })
    }
}

impl<'de> Deserialize<'de> for Row {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = <Vec<Value>>::deserialize(deserializer)?;
        Ok(Row { data })
    }
}

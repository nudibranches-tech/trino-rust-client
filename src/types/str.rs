use std::fmt;

use serde::de::{self, DeserializeSeed, Deserializer, Visitor};

use super::{Error, Trino, TrinoMapKey, TrinoTy};

impl<'b> Trino for &'b str {
    type ValueType<'a> = &'a str;
    type Seed<'a, 'de> = StrSeed;

    fn value(&self) -> Self::ValueType<'_> {
        *self
    }
    fn ty() -> TrinoTy {
        TrinoTy::Varchar
    }

    fn seed<'a, 'de>(_ty: &'a TrinoTy) -> Result<Self::Seed<'a, 'de>, Error> {
        Ok(StrSeed)
    }
}

impl<'b> TrinoMapKey for &'b str {}

pub struct StrSeed;

impl<'de> Visitor<'de> for StrSeed {
    type Value = &'de str;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("&str")
    }
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v)
    }
}

impl<'de> DeserializeSeed<'de> for StrSeed {
    type Value = &'de str;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(self)
    }
}

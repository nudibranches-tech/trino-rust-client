use std::fmt;
use std::marker::PhantomData;

use serde::de::{self, DeserializeSeed, Deserializer, Visitor};

use super::{Context, Trino, TrinoTy};

impl<T: Trino> Trino for Option<T> {
    type ValueType<'a>
        = Option<T::ValueType<'a>>
    where
        T: 'a;
    type Seed<'a, 'de> = OptionSeed<'a, T>;

    fn value(&self) -> Self::ValueType<'_> {
        self.as_ref().map(|t| t.value())
    }

    fn ty() -> TrinoTy {
        TrinoTy::Option(Box::new(T::ty()))
    }

    fn seed<'a, 'de>(ctx: &'a Context) -> Self::Seed<'a, 'de> {
        OptionSeed::new(ctx)
    }

    fn empty() -> Self {
        None
    }
}

pub struct OptionSeed<'a, T> {
    ctx: &'a Context<'a>,
    _marker: PhantomData<T>,
}

impl<'a, T> OptionSeed<'a, T> {
    // caller must provide a valid `Context`
    pub(super) fn new(ctx: &'a Context) -> Self {
        OptionSeed {
            ctx,
            _marker: PhantomData,
        }
    }
}

impl<'de, T: Trino> Visitor<'de> for OptionSeed<'_, T> {
    type Value = Option<T>;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(T::ty().raw_type().to_str())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let seed = T::seed(self.ctx);
        seed.deserialize(deserializer).map(Some)
    }
}

impl<'de, T: Trino> DeserializeSeed<'de> for OptionSeed<'_, T> {
    type Value = Option<T>;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_option(self)
    }
}

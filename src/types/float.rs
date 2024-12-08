use serde::de::{Deserialize, DeserializeSeed, Deserializer};

use super::{Context, Trino, TrinoFloat, TrinoMapKey, TrinoTy};

macro_rules! gen_float {
    ($ty:ty, $seed:ident, $pty:expr) => {
        impl Trino for $ty {
            type ValueType<'a> = &'a $ty;
            type Seed<'a, 'de> = $seed;

            fn value(&self) -> Self::ValueType<'_> {
                self
            }

            fn ty() -> TrinoTy {
                $pty
            }

            fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
                $seed
            }

            fn empty() -> Self {
                Default::default()
            }
        }

        impl TrinoMapKey for $ty {}

        pub struct $seed;

        impl<'de> DeserializeSeed<'de> for $seed {
            type Value = $ty;
            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::Value::deserialize(deserializer)
            }
        }
    };
}

use TrinoFloat::*;
gen_float!(f32, F32Seed, TrinoTy::TrinoFloat(F32));
gen_float!(f64, F64Seed, TrinoTy::TrinoFloat(F64));

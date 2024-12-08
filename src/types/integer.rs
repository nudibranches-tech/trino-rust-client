use serde::de::{Deserialize, DeserializeSeed, Deserializer};

use super::{Context, Trino, PrestoInt, TrinoMapKey, TrinoTy};

macro_rules! gen_int {
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

use PrestoInt::*;
gen_int!(i8, I8Seed, TrinoTy::PrestoInt(I8));
gen_int!(i16, I16Seed, TrinoTy::PrestoInt(I16));
gen_int!(i32, I32Seed, TrinoTy::PrestoInt(I32));
gen_int!(i64, I64Seed, TrinoTy::PrestoInt(I64));

//TODO: u64's presto type is i64, it may > i64::max, same as u8, u16, u32
gen_int!(u8, U8Seed, TrinoTy::PrestoInt(I8));
gen_int!(u16, U16Seed, TrinoTy::PrestoInt(I16));
gen_int!(u32, U32Seed, TrinoTy::PrestoInt(I32));
gen_int!(u64, U64Seed, TrinoTy::PrestoInt(I64));

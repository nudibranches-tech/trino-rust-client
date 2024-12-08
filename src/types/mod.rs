mod boolean;
mod data_set;
mod date_time;
mod decimal;
mod fixed_char;
mod float;
mod integer;
mod interval_day_to_second;
mod interval_year_to_month;
mod ip_address;
pub mod json;
mod map;
mod option;
mod row;
mod seq;
mod string;
mod util;
pub mod uuid;

pub use self::uuid::*;
pub use boolean::*;
pub use data_set::*;
pub use date_time::*;
pub use decimal::*;
pub use fixed_char::*;
pub use float::*;
pub use integer::*;
pub use interval_day_to_second::*;
pub use interval_year_to_month::*;
pub use ip_address::*;
pub use map::*;
pub use option::*;
pub use row::*;
pub use seq::*;
pub use string::*;

//mod str;
//pub use self::str::*;

use std::borrow::Cow;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::sync::Arc;

use crate::{
    ClientTypeSignatureParameter, Column, NamedTypeSignature, RawTrinoTy, RowFieldName,
    TypeSignature,
};
use derive_more::Display;
use iterable::*;
use serde::de::DeserializeSeed;
use serde::Serialize;

//TODO: refine it
#[derive(Display, Debug)]
pub enum Error {
    InvalidTrinoType,
    InvalidColumn,
    InvalidTypeSignature,
    ParseDecimalFailed(String),
    ParseIntervalMonthFailed,
    ParseIntervalDayFailed,
    EmptyInTrinoRow,
    NoneTrinoRow,
}

pub trait Trino {
    type ValueType<'a>: Serialize
    where
        Self: 'a;
    type Seed<'a, 'de>: DeserializeSeed<'de, Value = Self>;

    fn value(&self) -> Self::ValueType<'_>;

    fn ty() -> TrinoTy;

    /// caller must provide a valid context
    fn seed<'a, 'de>(ctx: &'a Context<'a>) -> Self::Seed<'a, 'de>;

    fn empty() -> Self;
}

pub trait TrinoMapKey: Trino {}

#[derive(Debug)]
pub struct Context<'a> {
    ty: &'a TrinoTy,
    map: Arc<HashMap<usize, Vec<usize>>>,
}

impl<'a> Context<'a> {
    pub fn new<T: Trino>(provided: &'a TrinoTy) -> Result<Self, Error> {
        let target = T::ty();
        let ret = extract(&target, provided)?;
        let map = HashMap::from_iter(ret);
        Ok(Context {
            ty: provided,
            map: Arc::new(map),
        })
    }

    pub fn with_ty(&'a self, ty: &'a TrinoTy) -> Context<'a> {
        Context {
            ty,
            map: self.map.clone(),
        }
    }

    pub fn ty(&self) -> &TrinoTy {
        self.ty
    }

    pub fn row_map(&self) -> Option<&[usize]> {
        let key = self.ty as *const TrinoTy as usize;
        self.map.get(&key).map(|r| &**r)
    }
}

fn extract(target: &TrinoTy, provided: &TrinoTy) -> Result<Vec<(usize, Vec<usize>)>, Error> {
    use TrinoTy::*;

    match (target, provided) {
        (Unknown, _) => Ok(vec![]),
        (Decimal(p1, s1), Decimal(p2, s2)) if p1 == p2 && s1 == s2 => Ok(vec![]),
        (Option(ty), provided) => extract(ty, provided),
        (Boolean, Boolean) => Ok(vec![]),
        (Date, Date) => Ok(vec![]),
        (Time, Time) => Ok(vec![]),
        (TimeWithTimeZone, TimeWithTimeZone) => Ok(vec![]),
        (Timestamp, Timestamp) => Ok(vec![]),
        (TimestampWithTimeZone, TimestampWithTimeZone) => Ok(vec![]),
        (IntervalYearToMonth, IntervalYearToMonth) => Ok(vec![]),
        (IntervalDayToSecond, IntervalDayToSecond) => Ok(vec![]),
        (TrinoInt(_), TrinoInt(_)) => Ok(vec![]),
        (TrinoFloat(_), TrinoFloat(_)) => Ok(vec![]),
        (Varchar, Varchar) => Ok(vec![]),
        (Char(a), Char(b)) if a == b => Ok(vec![]),
        (Tuple(t1), Tuple(t2)) => {
            if t1.len() != t2.len() {
                Err(Error::InvalidTrinoType)
            } else {
                t1.lazy_zip(t2).try_flat_map(|(l, r)| extract(l, r))
            }
        }
        (Row(t1), Row(t2)) => {
            if t1.len() != t2.len() {
                Err(Error::InvalidTrinoType)
            } else {
                // create a vector of the original element's reference
                let t1k = t1.sorted_by(|t1, t2| Ord::cmp(&t1.0, &t2.0));
                let t2k = t2.sorted_by(|t1, t2| Ord::cmp(&t1.0, &t2.0));

                let ret = t1k.lazy_zip(t2k).try_flat_map(|(l, r)| {
                    if l.0 == r.0 {
                        extract(&l.1, &r.1)
                    } else {
                        Err(Error::InvalidTrinoType)
                    }
                })?;

                let map = t2.map(|provided| t1.position(|target| provided.0 == target.0).unwrap());
                let key = provided as *const TrinoTy as usize;
                Ok(ret.add_one((key, map)))
            }
        }
        (Array(t1), Array(t2)) => extract(t1, t2),
        (Map(t1k, t1v), Map(t2k, t2v)) => Ok(extract(t1k, t2k)?.chain(extract(t1v, t2v)?)),
        (IpAddress, IpAddress) => Ok(vec![]),
        (Uuid, Uuid) => Ok(vec![]),
        (Json, Json) => Ok(vec![]),
        _ => Err(Error::InvalidTrinoType),
    }
}

// TODO:
// VarBinary Json
// TimestampWithTimeZone TimeWithTimeZone
// HyperLogLog P4HyperLogLog
// QDigest
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TrinoTy {
    Date,
    Time,
    TimeWithTimeZone,
    Timestamp,
    TimestampWithTimeZone,
    Uuid,
    IntervalYearToMonth,
    IntervalDayToSecond,
    Option(Box<TrinoTy>),
    Boolean,
    TrinoInt(TrinoInt),
    TrinoFloat(TrinoFloat),
    Varchar,
    Char(usize),
    Tuple(Vec<TrinoTy>),
    Row(Vec<(String, TrinoTy)>),
    Array(Box<TrinoTy>),
    Map(Box<TrinoTy>, Box<TrinoTy>),
    Decimal(usize, usize),
    IpAddress,
    Json,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TrinoInt {
    I8,
    I16,
    I32,
    I64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TrinoFloat {
    F32,
    F64,
}

impl TrinoTy {
    pub fn from_type_signature(mut sig: TypeSignature) -> Result<Self, Error> {
        use TrinoFloat::*;
        use TrinoInt::*;

        let ty = match sig.raw_type {
            RawTrinoTy::Date => TrinoTy::Date,
            RawTrinoTy::Time => TrinoTy::Time,
            RawTrinoTy::TimeWithTimeZone => TrinoTy::TimeWithTimeZone,
            RawTrinoTy::Timestamp => TrinoTy::Timestamp,
            RawTrinoTy::TimestampWithTimeZone => TrinoTy::TimestampWithTimeZone,
            RawTrinoTy::IntervalYearToMonth => TrinoTy::IntervalYearToMonth,
            RawTrinoTy::IntervalDayToSecond => TrinoTy::IntervalDayToSecond,
            RawTrinoTy::Unknown => TrinoTy::Unknown,
            RawTrinoTy::Decimal if sig.arguments.len() == 2 => {
                let s_sig = sig.arguments.pop().unwrap();
                let p_sig = sig.arguments.pop().unwrap();
                if let (
                    ClientTypeSignatureParameter::LongLiteral(p),
                    ClientTypeSignatureParameter::LongLiteral(s),
                ) = (p_sig, s_sig)
                {
                    TrinoTy::Decimal(p as usize, s as usize)
                } else {
                    return Err(Error::InvalidTypeSignature);
                }
            }
            RawTrinoTy::Boolean => TrinoTy::Boolean,
            RawTrinoTy::TinyInt => TrinoTy::TrinoInt(I8),
            RawTrinoTy::SmallInt => TrinoTy::TrinoInt(I16),
            RawTrinoTy::Integer => TrinoTy::TrinoInt(I32),
            RawTrinoTy::BigInt => TrinoTy::TrinoInt(I64),
            RawTrinoTy::Real => TrinoTy::TrinoFloat(F32),
            RawTrinoTy::Double => TrinoTy::TrinoFloat(F64),
            RawTrinoTy::VarChar => TrinoTy::Varchar,
            RawTrinoTy::Char if sig.arguments.len() == 1 => {
                if let ClientTypeSignatureParameter::LongLiteral(p) = sig.arguments.pop().unwrap() {
                    TrinoTy::Char(p as usize)
                } else {
                    return Err(Error::InvalidTypeSignature);
                }
            }
            RawTrinoTy::Array if sig.arguments.len() == 1 => {
                let sig = sig.arguments.pop().unwrap();
                if let ClientTypeSignatureParameter::TypeSignature(sig) = sig {
                    let inner = Self::from_type_signature(sig)?;
                    TrinoTy::Array(Box::new(inner))
                } else {
                    return Err(Error::InvalidTypeSignature);
                }
            }
            RawTrinoTy::Map if sig.arguments.len() == 2 => {
                let v_sig = sig.arguments.pop().unwrap();
                let k_sig = sig.arguments.pop().unwrap();
                if let (
                    ClientTypeSignatureParameter::TypeSignature(k_sig),
                    ClientTypeSignatureParameter::TypeSignature(v_sig),
                ) = (k_sig, v_sig)
                {
                    let k_inner = Self::from_type_signature(k_sig)?;
                    let v_inner = Self::from_type_signature(v_sig)?;
                    TrinoTy::Map(Box::new(k_inner), Box::new(v_inner))
                } else {
                    return Err(Error::InvalidTypeSignature);
                }
            }
            RawTrinoTy::Row if !sig.arguments.is_empty() => {
                let ir = sig.arguments.try_map(|arg| match arg {
                    ClientTypeSignatureParameter::NamedTypeSignature(sig) => {
                        let name = sig.field_name.map(|n| n.name);
                        let ty = Self::from_type_signature(sig.type_signature)?;
                        Ok((name, ty))
                    }
                    _ => Err(Error::InvalidTypeSignature),
                })?;

                let is_named = ir[0].0.is_some();

                if is_named {
                    let row = ir.try_map(|(name, ty)| match name {
                        Some(n) => Ok((n, ty)),
                        None => Err(Error::InvalidTypeSignature),
                    })?;
                    TrinoTy::Row(row)
                } else {
                    let tuple = ir.try_map(|(name, ty)| match name {
                        Some(_) => Err(Error::InvalidTypeSignature),
                        None => Ok(ty),
                    })?;
                    TrinoTy::Tuple(tuple)
                }
            }
            RawTrinoTy::IpAddress => TrinoTy::IpAddress,
            RawTrinoTy::Uuid => TrinoTy::Uuid,
            RawTrinoTy::Json => TrinoTy::Json,
            _ => return Err(Error::InvalidTypeSignature),
        };

        Ok(ty)
    }

    pub fn from_column(column: Column) -> Result<(String, Self), Error> {
        let name = column.name;
        if let Some(sig) = column.type_signature {
            let ty = Self::from_type_signature(sig)?;
            Ok((name, ty))
        } else {
            Err(Error::InvalidColumn)
        }
    }

    pub fn from_columns(columns: Vec<Column>) -> Result<Self, Error> {
        let row = columns.try_map(Self::from_column)?;
        Ok(TrinoTy::Row(row))
    }

    pub fn into_type_signature(self) -> TypeSignature {
        use TrinoTy::*;

        let raw_ty = self.raw_type();

        let params = match self {
            Unknown => vec![],
            Decimal(p, s) => vec![
                ClientTypeSignatureParameter::LongLiteral(p as u64),
                ClientTypeSignatureParameter::LongLiteral(s as u64),
            ],
            Date => vec![],
            Time => vec![],
            TimeWithTimeZone => vec![],
            Timestamp => vec![],
            TimestampWithTimeZone => vec![],
            IntervalYearToMonth => vec![],
            IntervalDayToSecond => vec![],
            Option(t) => return t.into_type_signature(),
            Boolean => vec![],
            TrinoInt(_) => vec![],
            TrinoFloat(_) => vec![],
            Varchar => vec![ClientTypeSignatureParameter::LongLiteral(2147483647)],
            Char(a) => vec![ClientTypeSignatureParameter::LongLiteral(a as u64)],
            Tuple(ts) => ts.map(|ty| {
                ClientTypeSignatureParameter::NamedTypeSignature(NamedTypeSignature {
                    field_name: None,
                    type_signature: ty.into_type_signature(),
                })
            }),
            Row(ts) => ts.map(|(name, ty)| {
                ClientTypeSignatureParameter::NamedTypeSignature(NamedTypeSignature {
                    field_name: Some(RowFieldName::new(name)),
                    type_signature: ty.into_type_signature(),
                })
            }),
            Array(t) => vec![ClientTypeSignatureParameter::TypeSignature(
                t.into_type_signature(),
            )],
            Map(t1, t2) => vec![
                ClientTypeSignatureParameter::TypeSignature(t1.into_type_signature()),
                ClientTypeSignatureParameter::TypeSignature(t2.into_type_signature()),
            ],
            IpAddress => vec![],
            Uuid => vec![],
            Json => vec![],
        };

        TypeSignature::new(raw_ty, params)
    }

    pub fn full_type(&self) -> Cow<'static, str> {
        use TrinoTy::*;

        match self {
            Unknown => RawTrinoTy::Unknown.to_str().into(),
            Decimal(p, s) => format!("{}({},{})", RawTrinoTy::Decimal.to_str(), p, s).into(),
            Option(t) => t.full_type(),
            Date => RawTrinoTy::Date.to_str().into(),
            Time => RawTrinoTy::Time.to_str().into(),
            TimeWithTimeZone => RawTrinoTy::TimeWithTimeZone.to_str().into(),
            Timestamp => RawTrinoTy::Timestamp.to_str().into(),
            TimestampWithTimeZone => RawTrinoTy::TimestampWithTimeZone.to_str().into(),
            IntervalYearToMonth => RawTrinoTy::IntervalYearToMonth.to_str().into(),
            IntervalDayToSecond => RawTrinoTy::IntervalDayToSecond.to_str().into(),
            Boolean => RawTrinoTy::Boolean.to_str().into(),
            TrinoInt(ty) => ty.raw_type().to_str().into(),
            TrinoFloat(ty) => ty.raw_type().to_str().into(),
            Varchar => RawTrinoTy::VarChar.to_str().into(),
            Char(a) => format!("{}({})", RawTrinoTy::Char.to_str(), a).into(),
            Tuple(ts) => format!(
                "{}({})",
                RawTrinoTy::Row.to_str(),
                ts.lazy_map(|ty| ty.full_type()).join(",")
            )
            .into(),
            Row(ts) => format!(
                "{}({})",
                RawTrinoTy::Row.to_str(),
                ts.lazy_map(|(name, ty)| format!("{} {}", name, ty.full_type()))
                    .join(",")
            )
            .into(),
            Array(t) => format!("{}({})", RawTrinoTy::Array.to_str(), t.full_type()).into(),
            Map(t1, t2) => format!(
                "{}({},{})",
                RawTrinoTy::Map.to_str(),
                t1.full_type(),
                t2.full_type()
            )
            .into(),
            IpAddress => RawTrinoTy::IpAddress.to_str().into(),
            Uuid => RawTrinoTy::Uuid.to_str().into(),
            Json => RawTrinoTy::Json.to_str().into(),
        }
    }

    pub fn raw_type(&self) -> RawTrinoTy {
        use TrinoTy::*;

        match self {
            Unknown => RawTrinoTy::Unknown,
            Date => RawTrinoTy::Date,
            Time => RawTrinoTy::Time,
            TimeWithTimeZone => RawTrinoTy::TimeWithTimeZone,
            Timestamp => RawTrinoTy::Timestamp,
            TimestampWithTimeZone => RawTrinoTy::TimestampWithTimeZone,
            IntervalYearToMonth => RawTrinoTy::IntervalYearToMonth,
            IntervalDayToSecond => RawTrinoTy::IntervalDayToSecond,
            Decimal(_, _) => RawTrinoTy::Decimal,
            Option(ty) => ty.raw_type(),
            Boolean => RawTrinoTy::Boolean,
            TrinoInt(ty) => ty.raw_type(),
            TrinoFloat(ty) => ty.raw_type(),
            Varchar => RawTrinoTy::VarChar,
            Char(_) => RawTrinoTy::Char,
            Tuple(_) => RawTrinoTy::Row,
            Row(_) => RawTrinoTy::Row,
            Array(_) => RawTrinoTy::Array,
            Map(_, _) => RawTrinoTy::Map,
            IpAddress => RawTrinoTy::IpAddress,
            Uuid => RawTrinoTy::Uuid,
            Json => RawTrinoTy::Json,
        }
    }
}

impl TrinoInt {
    pub fn raw_type(&self) -> RawTrinoTy {
        use TrinoInt::*;
        match self {
            I8 => RawTrinoTy::TinyInt,
            I16 => RawTrinoTy::SmallInt,
            I32 => RawTrinoTy::Integer,
            I64 => RawTrinoTy::BigInt,
        }
    }
}

impl TrinoFloat {
    pub fn raw_type(&self) -> RawTrinoTy {
        use TrinoFloat::*;
        match self {
            F32 => RawTrinoTy::Real,
            F64 => RawTrinoTy::Double,
        }
    }
}

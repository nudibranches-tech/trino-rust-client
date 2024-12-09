use std::fmt;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawTrinoTy {
    BigInt,
    Integer,
    SmallInt,
    TinyInt,
    Boolean,
    Date,
    Decimal,
    Real,
    Double,
    HyperLogLog,
    QDigest,
    P4HyperLogLog,
    IntervalDayToSecond,
    IntervalYearToMonth,
    Timestamp,
    TimestampWithTimeZone,
    Time,
    TimeWithTimeZone,
    VarBinary,
    VarChar,
    Char,
    Row,
    Array,
    Map,
    Json,
    IpAddress,
    Uuid,
    Unknown,
}

impl RawTrinoTy {
    pub fn to_str(&self) -> &'static str {
        use RawTrinoTy::*;
        match *self {
            BigInt => "bigint",
            Integer => "integer",
            SmallInt => "smallint",
            TinyInt => "tinyint",
            Boolean => "boolean",
            Date => "date",
            Decimal => "decimal",
            Real => "real",
            Double => "double",
            HyperLogLog => "HyperLogLog",
            QDigest => "qdigest",
            P4HyperLogLog => "P4HyperLogLog",
            IntervalDayToSecond => "interval day to second",
            IntervalYearToMonth => "interval year to month",
            Timestamp => "timestamp",
            TimestampWithTimeZone => "timestamp with time zone",
            Time => "time",
            TimeWithTimeZone => "time with time zone",
            VarBinary => "varbinary",
            VarChar => "varchar",
            Char => "char",
            Row => "row",
            Array => "array",
            Map => "map",
            Json => "json",
            IpAddress => "ipaddress",
            Uuid => "uuid",
            Unknown => "unknown",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        use RawTrinoTy::*;
        let ty = match s {
            "bigint" => BigInt,
            "integer" => Integer,
            "smallint" => SmallInt,
            "tinyint" => TinyInt,
            "boolean" => Boolean,
            "date" => Date,
            "decimal" => Decimal,
            "real" => Real,
            "double" => Double,
            "HyperLogLog" => HyperLogLog,
            "qdigest" => QDigest,
            "P4HyperLogLog" => P4HyperLogLog,
            "interval day to second" => IntervalDayToSecond,
            "interval year to month" => IntervalYearToMonth,
            "timestamp" => Timestamp,
            "timestamp with time zone" => TimestampWithTimeZone,
            "time" => Time,
            "time with time zone" => TimeWithTimeZone,
            "varbinary" => VarBinary,
            "varchar" => VarChar,
            "char" => Char,
            "row" => Row,
            "array" => Array,
            "map" => Map,
            "json" => Json,
            "ipaddress" => IpAddress,
            "uuid" => Uuid,
            "unknown" => Unknown,
            _ => return None,
        };
        Some(ty)
    }
}

impl Serialize for RawTrinoTy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_str())
    }
}

impl<'de> Deserialize<'de> for RawTrinoTy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TyVistor;

        impl Visitor<'_> for TyVistor {
            type Value = RawTrinoTy;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("need str")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match RawTrinoTy::parse(v) {
                    Some(d) => Ok(d),
                    None => Err(E::custom(format!("invalid trino type: {}", v))),
                }
            }
        }

        deserializer.deserialize_str(TyVistor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ser() {
        let ty = RawTrinoTy::Char;
        let s = serde_json::to_string(&ty).unwrap();
        assert_eq!(s, "\"char\"");

        let ty = RawTrinoTy::Json;
        let s = serde_json::to_string(&ty).unwrap();
        assert_eq!(s, "\"json\"");
    }

    #[test]
    fn test_de() {
        let data = "\"char\"";
        let ty = serde_json::from_str::<RawTrinoTy>(data).unwrap();
        assert_eq!(ty, RawTrinoTy::Char);

        let data = "\"json\"";
        let ty = serde_json::from_str::<RawTrinoTy>(data).unwrap();
        assert_eq!(ty, RawTrinoTy::Json);

        let invalid = "\"xxx\"";
        let res = serde_json::from_str::<RawTrinoTy>(invalid);
        assert!(res.is_err());
    }
}

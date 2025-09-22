use std::fmt;
use std::marker::PhantomData;

use iterable::Iterable;
use serde::de::{self, Deserializer, MapAccess, Visitor};
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};

use super::util::SerializeIterator;
use super::{Context, Error, Trino, TrinoTy, VecSeed};
use crate::models::Column;
use crate::Row;

#[derive(Debug)]
pub struct DataSet<T: Trino> {
    types: Vec<(String, TrinoTy)>,
    data: Vec<T>,
}

impl<T: Trino> DataSet<T> {
    pub fn new(data: Vec<T>) -> Result<Self, Error> {
        let types = match T::ty() {
            TrinoTy::Row(r) => {
                if r.is_empty() {
                    return Err(Error::EmptyInTrinoRow);
                } else {
                    r
                }
            }
            _ => return Err(Error::NoneTrinoRow),
        };

        Ok(DataSet { types, data })
    }

    pub fn split(self) -> (Vec<(String, TrinoTy)>, Vec<T>) {
        (self.types, self.data)
    }

    pub fn into_vec(self) -> Vec<T> {
        self.data
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn as_slice(&self) -> &[T] {
        self.data.as_slice()
    }

    pub fn merge(&mut self, other: DataSet<T>) {
        self.data.extend(other.data)
    }
}

impl DataSet<Row> {
    pub fn new_row(types: Vec<(String, TrinoTy)>, data: Vec<Row>) -> Result<Self, Error> {
        if types.is_empty() {
            return Err(Error::EmptyInTrinoRow);
        }
        Ok(DataSet { types, data })
    }
}

impl<T: Trino + Clone> Clone for DataSet<T> {
    fn clone(&self) -> Self {
        DataSet {
            types: self.types.clone(),
            data: self.data.clone(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////
// Serialize

impl<T: Trino> Serialize for DataSet<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("DataSet", 2)?;

        let columns = self.types.clone().map(|(name, ty)| Column {
            name,
            ty: ty.full_type().into_owned(),
            type_signature: Some(ty.into_type_signature()),
        });

        let data = SerializeIterator {
            iter: self.data.iter().map(|d| d.value()),
            size: Some(self.data.len()),
        };
        state.serialize_field("columns", &columns)?;
        state.serialize_field("data", &data)?;
        state.end()
    }
}

///////////////////////////////////////////////////////////////////////////////////
// Deserialize

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "lowercase")]
enum Field {
    Columns,
    Data,
}

const FIELDS: &[&str] = &["columns", "data"];

impl<'de, T: Trino> Deserialize<'de> for DataSet<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DataSetVisitor<T: Trino>(PhantomData<T>);

        impl<'de, T: Trino> Visitor<'de> for DataSetVisitor<T> {
            type Value = DataSet<T>;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct DataSet")
            }

            fn visit_map<V>(self, mut map: V) -> Result<DataSet<T>, V::Error>
            where
                V: MapAccess<'de>,
            {
                let types = if let Some(Field::Columns) = map.next_key()? {
                    let columns: Vec<Column> = map.next_value()?;
                    columns.try_map(TrinoTy::from_column).map_err(|e| {
                        de::Error::custom(format!("deserialize trino type failed, reason: {}", e))
                    })?
                } else {
                    return Err(de::Error::missing_field("columns"));
                };

                if types.is_empty() {
                    // For empty columns (like PREPARE statements), skip data field processing
                    // and just consume the data field if it exists
                    if let Some(Field::Data) = map.next_key()? {
                        let _: serde_json::Value = map.next_value()?; // consume and ignore
                    }
                    if let Some(Field::Columns) = map.next_key()? {
                        return Err(de::Error::duplicate_field("columns"));
                    }
                    return Ok(DataSet {
                        types,
                        data: vec![],
                    });
                }

                let array_ty = TrinoTy::Array(Box::new(TrinoTy::Row(types.clone())));
                let ctx = Context::new::<Vec<T>>(&array_ty)
                    .map_err(|e| de::Error::custom(format!("invalid trino type, reason: {}", e)))?;
                let seed = VecSeed::new(&ctx);

                let data = if let Some(Field::Data) = map.next_key()? {
                    map.next_value_seed(seed)?
                } else {
                    // it is empty when there is no data
                    vec![]
                };

                match map.next_key::<Field>()? {
                    Some(Field::Columns) => return Err(de::Error::duplicate_field("columns")),
                    Some(Field::Data) => return Err(de::Error::duplicate_field("data")),
                    None => {}
                }

                if let TrinoTy::Unknown = T::ty() {
                    Ok(DataSet { types, data })
                } else {
                    DataSet::new(data).map_err(|e| {
                        de::Error::custom(format!("construct data failed, reason: {}", e))
                    })
                }
            }
        }

        deserializer.deserialize_struct("DataSet", FIELDS, DataSetVisitor(PhantomData))
    }
}

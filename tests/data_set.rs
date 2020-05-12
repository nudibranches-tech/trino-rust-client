#![allow(incomplete_features)]
#![feature(generic_associated_types)]

use std::collections::*;
use std::fs::File;
use std::io::Read;
use std::iter::FromIterator;

use maplit::hashmap;
use serde_json::value::Value;

use presto::types::DataSet;
use presto::Column;
use presto::Presto;

fn read(name: &str) -> (String, Value) {
    let p = "tests/data/".to_string() + name;
    let mut f = File::open(p).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();

    let v = serde_json::from_str(&buf).unwrap();
    (buf, v)
}

fn assert_ds<T: Presto>(data_set: DataSet<T>, v: Value) {
    let data_set = serde_json::to_value(data_set).unwrap();
    let (l_meta, l_data) = split(data_set).unwrap();
    let (r_meta, r_data) = split(v).unwrap();

    assert_eq!(l_meta, r_meta);
    assert_eq!(l_data, r_data);
}

// return (meta, data)
fn split(v: Value) -> Option<(Vec<Column>, Value)> {
    if let Value::Object(m) = v {
        if m.len() == 2 {
            let meta = m.get("columns")?.clone();
            let meta = serde_json::from_value(meta).ok()?;
            let data = m.get("data")?.clone();
            Some((meta, data))
        } else {
            None
        }
    } else {
        None
    }
}

#[test]
fn test_option() {
    #[derive(Presto, Eq, PartialEq, Debug, Clone)]
    struct A {
        a: String,
        b: Option<String>,
    }

    let (s, v) = read("option");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 3);
    assert_eq!(
        d[0],
        A {
            a: "a".to_string(),
            b: None,
        }
    );
    assert_eq!(
        d[1],
        A {
            a: "b".to_string(),
            b: Some("Some(b)".to_string()),
        }
    );
    assert_eq!(
        d[2],
        A {
            a: "c".to_string(),
            b: None,
        }
    );
}

#[test]
fn test_seq() {
    #[derive(Presto, Debug, Clone)]
    struct A {
        a: Vec<i32>,
        b: LinkedList<i32>,
        c: VecDeque<i32>,
    }

    let (s, v) = read("seq");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let mut d = d.into_vec();
    assert_eq!(d.len(), 1);

    let d = d.pop().unwrap();
    assert_eq!(d.a, vec![1, 2, 3]);
    assert_eq!(d.b, LinkedList::from_iter(vec![1, 2, 3]));
    assert_eq!(d.c, VecDeque::from_iter(vec![1, 2, 3]));
}

#[test]
fn test_seq_other() {
    #[derive(Presto, Debug, Clone)]
    struct A {
        a: HashSet<i32>,
        b: BTreeSet<i32>,
        c: BinaryHeap<i32>,
    }

    let (s, _) = read("seq");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();

    let mut d = d.into_vec();
    assert_eq!(d.len(), 1);

    let mut d = d.pop().unwrap();
    assert_eq!(d.a, HashSet::from_iter(vec![1, 2, 3]));
    assert_eq!(d.b, BTreeSet::from_iter(vec![1, 2, 3]));

    assert_eq!(d.c.pop(), Some(3));
    assert_eq!(d.c.pop(), Some(2));
    assert_eq!(d.c.pop(), Some(1));
    assert_eq!(d.c.pop(), None);
}

#[test]
fn test_map() {
    #[derive(Presto, Eq, PartialEq, Debug, Clone)]
    struct A {
        a: HashMap<String, i32>,
        b: i32,
    }

    let (s, v) = read("map");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 1);
    assert_eq!(
        d[0],
        A {
            a: hashmap![
                "foo".to_string() => 1,
                "bar".to_string() => 2,
            ],
            b: 5,
        }
    );
}

#[test]
fn test_row() {
    #[derive(Presto, Eq, PartialEq, Debug, Clone)]
    struct A {
        a: B,
        b: i32,
    }

    #[derive(Presto, Eq, PartialEq, Debug, Clone)]
    struct B {
        x: i32,
        y: i32,
    }

    let (s, v) = read("row");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 1);
    assert_eq!(
        d[0],
        A {
            a: B { x: 1, y: 1 },
            b: 5,
        }
    );
}

#[test]
fn test_integer() {
    #[derive(Presto, Eq, PartialEq, Debug, Clone)]
    struct A {
        a: i8,
        b: i16,
        c: i32,
        d: i64,
        e: u64,
        f: u16,
        g: u32,
        h: u8,
    }

    let (s, v) = read("integer");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 1);
    assert_eq!(
        d[0],
        A {
            a: -4,
            b: -3,
            c: -2,
            d: -1,
            e: 1,
            f: 2,
            g: 3,
            h: 4,
        }
    );
}

#[test]
fn test_float() {
    #[derive(Presto, PartialEq, Debug, Clone)]
    struct A {
        a: f32,
        b: f64,
    }

    let (s, _) = read("float");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();

    let d = d.into_vec();
    assert_eq!(d.len(), 1);
    assert_eq!(
        d[0],
        A {
            a: -3_f32,
            b: -1_f64,
        }
    );
}
#![allow(incomplete_features)]

use trino_rust_client::types::{Context, TrinoTy};
use trino_rust_client::Trino;

#[derive(Trino)]
struct A {
    a: String,
    b: i32,
    c: String,
}

#[derive(Trino)]
struct B {
    b: i32,
    c: String,
    a: String,
}

#[derive(Trino)]
struct C {
    a: A,
    b: i32,
}

#[derive(Trino)]
struct D {
    b: i32,
    a: B,
}

#[test]
fn test_simple() {
    let provided = B::ty();
    let ctx = Context::new::<A>(&provided).unwrap();
    let ret = ctx.row_map().unwrap();

    assert_eq!(ret, &[1, 2, 0]);
}

#[test]
fn test_nested() {
    let provided = D::ty();
    let ctx = Context::new::<C>(&provided).unwrap();

    let ret = ctx.row_map().unwrap();
    assert_eq!(ret, &[1, 0]);

    if let TrinoTy::Row(rows) = &provided {
        assert_eq!(rows.len(), 2);

        let ty = &rows[1].1;
        let ctx = ctx.with_ty(ty);

        let ret = ctx.row_map().unwrap();
        assert_eq!(ret, &[1, 2, 0]);
    } else {
        unreachable!()
    }
}

#[test]
fn test_false() {
    let provided = C::ty();
    let res = Context::new::<B>(&provided);

    assert!(res.is_err());
}

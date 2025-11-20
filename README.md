# Trino rust client

A [trino](https://trino.io/) client library written in rust.

This project have been forked on 08/12/24 from the great : [prusto](https://github.com/nooberfsh/prusto)
made by @nooberfsh.

Fork rationale  :
- Remove presto support
- Add advanced trino features.
- Rename things as "trino"

## Features

### authn:
- Basic Auth
- Jwt Auth

### protocols:
- Spooling Protocol (for efficient large result set handling)

## Installation

```toml
# Cargo.toml
[dependencies]
trino-rust-client = "0.9.0"

# For spooling protocol support
trino-rust-client = { version = "0.9.0", features = ["spooling"] }
```

## Example

### Basic example
```rust
use trino_rust_client::{ClientBuilder, Trino};

#[derive(Trino, Debug)]
struct Foo {
    a: i64,
    b: f64,
    c: String,
}

#[tokio::main]
async fn main() {
    let cli = ClientBuilder::new("user", "localhost")
        .port(8090)
        .catalog("catalog")
        .build()
        .unwrap();

    let sql = "select 1 as a, cast(1.1 as double) as b, 'bar' as c ";

    let data = cli.get_all::<Foo>(sql.into()).await.unwrap().into_vec();

    for r in data {
        println!("{:?}", r)
    }
}
```

### Https & Jwt example
```rust
use trino_rust_client::{ClientBuilder, Trino};

#[derive(Trino, Debug)]
struct Foo {
    a: i64,
    b: f64,
    c: String,
}

#[tokio::main]
async fn main() {
    let auth = Auth::Jwt("your access token");

    let cli = ClientBuilder::new("user", "localhost")
        .port(8443)
        .secure(true)
        .auth(auth)
        .catalog("catalog")
        .build()
        .unwrap();

    let sql = "select 1 as a, cast(1.1 as double) as b, 'bar' as c ";

    let data = cli.get_all::<Foo>(sql.into()).await.unwrap().into_vec();

    for r in data {
        println!("{:?}", r)
    }
}
```

### Example dealing with fields not known at compile time
```rust
use trino_rust_client::{ClientBuilder, Row, Trino};

#[tokio::main]
async fn main() {
    let cli = ClientBuilder::new("user", "localhost")
        .port(8080)
        .catalog("catalog")
        .build()
        .unwrap();

    let sql = "select first_name, last_name from users";

    let rows = cli.get_all::<Row>(sql.into()).await.unwrap().into_vec();

    for row in rows {
        let first_name = row.value().get(0).unwrap();
        let last_name = row.value().get(1).unwrap();
        println!("{} : {}", first_name, last_name);
    }
}
```

### Spooling Protocol example
```rust
use trino_rust_client::{ClientBuilder, Trino};

#[derive(Trino, Debug)]
struct User {
    id: i64,
    name: String,
    email: String,
}

#[tokio::main]
async fn main() {
    let cli = ClientBuilder::new("user", "localhost")
        .port(8080)
        .catalog("memory")
        .schema("default")
        .spooling_encoding("json+zstd")  // Enable spooling with compression
        .max_concurrent_segments(10)      // Optional: control concurrent downloads
        .build()
        .unwrap();

    let sql = "SELECT id, name, email FROM users LIMIT 1000";

    let data = cli.get_all::<User>(sql.into()).await.unwrap();

    println!("Retrieved {} rows", data.len());

    for user in data.as_slice() {
        println!("{:?}", user);
    }
}
```

## License

MIT

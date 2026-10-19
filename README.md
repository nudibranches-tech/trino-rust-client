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
- Interactive OAuth2 (browser-based)

### protocols:
- Spooling Protocol (for efficient large result set handling)

### tls:
- Selectable rustls crypto provider (`aws-lc-rs`, `ring`, or bring your own)

## Installation

```toml
# Cargo.toml
[dependencies]
trino-rust-client = "0.12.0"

# For spooling protocol support
trino-rust-client = { version = "0.12.0", features = ["spooling"] }
```

### TLS provider

HTTPS goes through [rustls](https://docs.rs/rustls); the cryptographic provider
behind it is picked with a cargo feature. One of them is required — a build with
none fails with a `compile_error!`.

| feature | provider | setup |
| --- | --- | --- |
| `rustls-aws-lc-rs` *(default)* | `aws-lc-rs` | none |
| `rustls-ring` | `ring` | none — installed as the process default provider on the first `ClientBuilder::build`, unless one is already installed |
| `rustls-no-provider` | yours | install a `CryptoProvider` before building a client — `reqwest` panics without one |

`aws-lc-rs` pulls in `aws-lc-sys`. To leave it out, turn the default feature off
and pick another provider:

```toml
[dependencies]
trino-rust-client = { version = "0.12.0", default-features = false, features = ["rustls-ring"] }
```

Turning the default feature off only pays off if nothing else in the graph
enables it: with both features on you get `aws-lc-sys` compiled *and* `ring`
installed as the process default provider, so the C build stays and `ring` is
what `reqwest` ends up using.

With `rustls-no-provider`, install the provider yourself before any client is
built. Depend on the same rustls major as `reqwest` (`0.23`), otherwise the
provider you install is not the one `reqwest` reads:

```toml
rustls = { version = "0.23", default-features = false, features = ["ring", "std"] }
```

```rust
rustls::crypto::ring::default_provider()
    .install_default()
    .expect("install rustls crypto provider");

let client = ClientBuilder::new("user", "localhost").build()?;
```

## Upgrading

Breaking changes between releases are documented with before/after examples in
the [migration guide](MIGRATION.md).

## Observability

The client emits [`tracing`](https://docs.rs/tracing) events and wraps each
`get_all` / `stream` / `execute` call in a span carrying the `query_id`, so
logs correlate per query. Install any subscriber to see them, e.g.:

```rust
tracing_subscriber::fmt()
    .with_env_filter("trino_rust_client=debug")
    .init();
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

### Interactive OAuth2 example

Trino's OAuth2 authentication makes the **coordinator** the OAuth client: on a
`401` the client opens the coordinator-supplied login URL in a browser (and
prints it to stderr as a fallback), polls Trino's token endpoint until you finish
the IdP login, then retries with the bearer token. The token is cached in memory
for the life of the `Client`. Requires TLS to the coordinator.

```rust
use trino_rust_client::auth::Auth;
use trino_rust_client::{ClientBuilder, Row};

#[tokio::main]
async fn main() {
    let cli = ClientBuilder::new("user", "coordinator.example.com")
        .secure(true)
        .auth(Auth::new_oauth2())
        .catalog("catalog")
        .build()
        .unwrap();

    let data = cli.get_all::<Row>("select 1").await.unwrap().into_vec();

    for r in data {
        println!("{:?}", r)
    }
}
```

Supply a custom presentation strategy (instead of opening a browser) with
`Auth::new_oauth2_with_handler(Arc::new(my_handler))`, and tune the token poll
loop with `.with_poll(max_attempts, timeout)`.

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

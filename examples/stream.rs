use std::env::var;

use dotenvy::dotenv;
use futures::StreamExt;
use trino_rust_client::{ClientBuilder, Row};

/// Stream a query's rows lazily instead of buffering the whole result set.
///
/// Run with the same environment variables as the other examples
/// (USER, HOST, PORT, CATALOG, SQL), e.g. via a `.env` file.
#[tokio::main]
async fn main() {
    dotenv().ok();

    let user = var("USER").unwrap();
    let host = var("HOST").unwrap();
    let port = var("PORT").unwrap().parse().unwrap();
    let catalog = var("CATALOG").unwrap();
    let sql = var("SQL").unwrap();

    let cli = ClientBuilder::new(user, host)
        .port(port)
        .catalog(catalog)
        .build()
        .unwrap();

    let mut stream = cli.stream::<Row>(sql).await.unwrap();
    println!("columns: {:?}", stream.columns());

    let mut count = 0usize;
    while let Some(row) = stream.next().await {
        match row {
            Ok(row) => {
                count += 1;
                println!("{:?}", row);
            }
            Err(e) => {
                eprintln!("stream error: {e}");
                break;
            }
        }
    }

    println!("streamed {count} rows");
}

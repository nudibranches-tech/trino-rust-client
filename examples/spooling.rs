use serde::{Deserialize, Serialize};
use trino_rust_client::{ClientBuilder, DataSet, Trino};

#[derive(Trino, Debug, Deserialize, Serialize)]
struct User {
    id: i64,
    name: String,
    email: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ClientBuilder::new("user", "localhost")
        .port(8080)
        .catalog("memory")
        .schema("default")
        .spooling_encoding("json+zstd")
        .max_concurrent_segments(10)
        .build()?;

    let sql = "SELECT id, name, email FROM users LIMIT 1000";

    println!("Executing query: {}", sql);
    let dataset: DataSet<User> = client.get_all::<User>(sql.to_string()).await?;

    println!("Retrieved {} rows", dataset.len());

    for (i, user) in dataset.as_slice().iter().take(5).enumerate() {
        println!(
            "Row {}: id={}, name={}, email={}",
            i + 1,
            user.id,
            user.name,
            user.email
        );
    }

    if dataset.len() > 5 {
        println!("... and {} more rows", dataset.len() - 5);
    }

    Ok(())
}

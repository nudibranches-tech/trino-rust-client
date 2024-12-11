use std::env::var;

use dotenv::dotenv;
use trino_rust_client::auth::Auth;
use trino_rust_client::ssl::Ssl;
use trino_rust_client::{ClientBuilder, Row};

#[tokio::main]
async fn main() {
    dotenv().ok();

    let user = var("UserName").unwrap(); //todo fixme
    let access_token = var("ACCESS_TOKEN").unwrap();
    let host = var("HOST").unwrap();
    let port = var("PORT").unwrap().parse().unwrap();
    let catalog = var("CATALOG").unwrap();
    let sql = var("SQL").unwrap();

    let auth = Auth::Jwt(access_token);
    let cli = ClientBuilder::new(user, host)
        .port(port)
        .catalog(catalog)
        .auth(auth)
        .secure(true)
        .ssl(Ssl {
            root_cert: Some(Ssl::read_pem(&"/path/root.pem").unwrap()),
        })
        .build()
        .unwrap();

    let data = cli.get_all::<Row>(sql).await.unwrap().into_vec();

    for r in data {
        println!("{:?}", r)
    }
}

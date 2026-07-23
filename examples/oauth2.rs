use std::env::var;

use dotenvy::dotenv;
use trino_rust_client::auth::Auth;
use trino_rust_client::{ClientBuilder, Row};

/// Interactive (browser-based) OAuth2 against a Trino coordinator configured with
/// `http-server.authentication.type=OAUTH2`.
///
/// On the first request the client receives a `401`, opens the coordinator's
/// login URL in your browser (and prints it to stderr as a fallback for
/// headless/SSH sessions), then polls Trino's token endpoint until you finish
/// the IdP login and retries the request with the bearer token. The token is
/// cached in memory for the life of the `Client`. Trino requires TLS for OAuth2,
/// so `.secure(true)` is mandatory.
///
/// Run with e.g. a `.env` file providing USERNAME/HOST/PORT/CATALOG/SQL:
///   cargo run --example oauth2
#[tokio::main]
async fn main() {
    dotenv().ok();

    let user = var("USERNAME").unwrap();
    let host = var("HOST").unwrap();
    let port = var("PORT")
        .unwrap_or_else(|_| "8443".into())
        .parse()
        .unwrap();
    let catalog = var("CATALOG").unwrap();
    let sql = var("SQL").unwrap();

    // Default handler: opens the system browser and prints the URL to stderr.
    // For a custom presentation strategy use
    // `Auth::new_oauth2_with_handler(Arc::new(my_handler))`, and tune the token
    // poll loop with `.with_poll(max_attempts, timeout)`.
    let auth = Auth::new_oauth2();

    let cli = ClientBuilder::new(user, host)
        .port(port)
        .catalog(catalog)
        .auth(auth)
        .secure(true) // OAuth2 requires HTTPS to the coordinator
        // For a self-signed coordinator certificate, also supply its root cert:
        //   .ssl(Ssl { root_cert: Some(Ssl::read_pem(&"/path/root.pem").unwrap()) })
        .build()
        .unwrap();

    let data = cli.get_all::<Row>(sql).await.unwrap().into_vec();

    for r in data {
        println!("{:?}", r)
    }
}

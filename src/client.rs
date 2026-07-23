use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use backon::ExponentialBuilder;
use backon::Retryable;
use futures::Stream;
use http::header::{ACCEPT_ENCODING, USER_AGENT};
use http::StatusCode;
use iterable::*;
use reqwest::header::HeaderValue;
use reqwest::{RequestBuilder, Response, Url};
use tokio::sync::RwLock;
use tracing::*;

use crate::auth::Auth;
use crate::build_dataset;
use crate::error::TrinoRetryResult;
use crate::error::{Error, Result};
use crate::header::*;
use crate::models::Column;
use crate::models::QueryResultData;
#[cfg(feature = "spooling")]
use crate::models::SpooledData;
use crate::retry::RetryPolicy;
use crate::selected_role::SelectedRole;
use crate::session::{Session, SessionBuilder};
#[cfg(feature = "spooling")]
use crate::spooling::decompress_segment_bytes;
#[cfg(feature = "spooling")]
use crate::spooling::{SegmentFetcher, SpoolingEncoding};
use crate::ssl::Ssl;
use crate::transaction::TransactionId;
use crate::{DataSet, QueryResult, Row, Trino};

// TODO:
// allow_redirects
// proxies

/// A configured Trino client.
///
/// Created with [`ClientBuilder`]. Cheap to share: it wraps a connection-pooled
/// HTTP client, so build one and reuse it for all queries. The main entry
/// points are [`get_all`](Client::get_all) (buffer the result),
/// [`stream`](Client::stream) (stream it lazily) and [`execute`](Client::execute)
/// (run a statement).
pub struct Client {
    client: reqwest::Client,
    session: RwLock<Session>,
    auth: Option<Auth>,
    retry: RetryPolicy,
    url: Url,
    #[cfg(feature = "spooling")]
    segment_fetcher: SegmentFetcher,
}

/// Builder for a [`Client`].
///
/// Start with [`ClientBuilder::new`], chain the setters you need, then call
/// [`build`](ClientBuilder::build).
///
/// ```no_run
/// # use trino_rust_client::client::ClientBuilder;
/// # fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let client = ClientBuilder::new("user", "trino.example.com")
///     .port(8443)
///     .secure(true)
///     .catalog("hive")
///     .schema("default")
///     .build()?;
/// # Ok(()) }
/// ```
pub struct ClientBuilder {
    session: SessionBuilder,
    auth: Option<Auth>,
    auth_http_insecure: bool,
    retry: RetryPolicy,
    ssl: Option<Ssl>,
    no_verify: bool,
    #[cfg(feature = "spooling")]
    segment_fetcher: Option<SegmentFetcher>,
    #[cfg(feature = "spooling")]
    max_concurrent_segments: Option<usize>,
}

/// Outcome of a statement run with [`Client::execute`].
#[derive(Debug)]
pub struct ExecuteResult {
    /// URI of the output, when the statement produces one.
    pub output_uri: Option<String>,
    /// The kind of update (e.g. `INSERT`, `CREATE TABLE`), if reported.
    pub update_type: Option<String>,
    /// Number of rows affected, if reported.
    pub update_count: Option<u64>,
}

impl ClientBuilder {
    /// Start building a client for the given Trino `user` and `host`.
    ///
    /// Defaults: port 8080, plain HTTP, no authentication. Use the setters to
    /// change them, then call [`build`](ClientBuilder::build).
    pub fn new(user: impl ToString, host: impl ToString) -> Self {
        let builder = SessionBuilder::new(user, host);
        Self {
            session: builder,
            auth: None,
            auth_http_insecure: false,
            retry: RetryPolicy::default(),
            ssl: None,
            no_verify: false,
            #[cfg(feature = "spooling")]
            segment_fetcher: None,
            #[cfg(feature = "spooling")]
            max_concurrent_segments: None,
        }
    }

    pub fn port(mut self, s: u16) -> Self {
        self.session.port = s;
        self
    }

    pub fn secure(mut self, s: bool) -> Self {
        self.session.secure = s;
        self
    }

    pub fn no_verify(mut self, nv: bool) -> Self {
        self.no_verify = nv;
        self
    }

    pub fn source(mut self, s: impl ToString) -> Self {
        self.session.source = s.to_string();
        self
    }

    pub fn trace_token(mut self, s: impl ToString) -> Self {
        self.session.trace_token = Some(s.to_string());
        self
    }

    pub fn client_tags(mut self, s: HashSet<String>) -> Self {
        self.session.client_tags = s;
        self
    }

    pub fn client_tag(mut self, s: impl ToString) -> Self {
        self.session.client_tags.insert(s.to_string());
        self
    }

    pub fn client_info(mut self, s: impl ToString) -> Self {
        self.session.client_info = Some(s.to_string());
        self
    }

    pub fn catalog(mut self, s: impl ToString) -> Self {
        self.session.catalog = Some(s.to_string());
        self
    }

    pub fn schema(mut self, s: impl ToString) -> Self {
        self.session.schema = Some(s.to_string());
        self
    }

    pub fn path(mut self, s: impl ToString) -> Self {
        self.session.path = Some(s.to_string());
        self
    }

    pub fn resource_estimates(mut self, s: HashMap<String, String>) -> Self {
        self.session.resource_estimates = s;
        self
    }

    pub fn resource_estimate(mut self, k: impl ToString, v: impl ToString) -> Self {
        self.session
            .resource_estimates
            .insert(k.to_string(), v.to_string());
        self
    }

    pub fn properties(mut self, s: HashMap<String, String>) -> Self {
        self.session.properties = s;
        self
    }

    pub fn property(mut self, k: impl ToString, v: impl ToString) -> Self {
        self.session.properties.insert(k.to_string(), v.to_string());
        self
    }

    pub fn prepared_statements(mut self, s: HashMap<String, String>) -> Self {
        self.session.prepared_statements = s;
        self
    }

    pub fn prepared_statement(mut self, k: impl ToString, v: impl ToString) -> Self {
        self.session
            .prepared_statements
            .insert(k.to_string(), v.to_string());
        self
    }

    pub fn extra_credentials(mut self, s: HashMap<String, String>) -> Self {
        self.session.extra_credentials = s;
        self
    }

    pub fn extra_credential(mut self, k: impl ToString, v: impl ToString) -> Self {
        self.session
            .extra_credentials
            .insert(k.to_string(), v.to_string());
        self
    }

    pub fn transaction_id(mut self, s: TransactionId) -> Self {
        self.session.transaction_id = s;
        self
    }

    pub fn client_request_timeout(mut self, s: Duration) -> Self {
        self.session.client_request_timeout = s;
        self
    }

    pub fn compression_disabled(mut self, s: bool) -> Self {
        self.session.compression_disabled = s;
        self
    }

    #[cfg(feature = "spooling")]
    pub fn segment_fetcher(mut self, segment_fetcher: SegmentFetcher) -> Self {
        self.segment_fetcher = Some(segment_fetcher);
        self
    }

    #[cfg(feature = "spooling")]
    /// Set the maximum number of concurrent segment fetches
    /// Default is based on available CPU parallelism (minimum 1)
    pub fn max_concurrent_segments(mut self, count: usize) -> Self {
        self.max_concurrent_segments = Some(count);
        self
    }

    #[cfg(feature = "spooling")]
    /// Set the spooling encoding format. Supported values: "json", "json+zstd", "json+lz4".
    /// Defaults to "json+zstd" if not specified.
    pub fn spooling_encoding(mut self, encoding: impl ToString) -> Self {
        let encoding_str = encoding.to_string();

        match SpoolingEncoding::try_from(encoding_str.as_str()) {
            Ok(_) => {
                self.session.spooling_encoding = Some(encoding_str);
            }
            Err(_) => {
                tracing::warn!(
                    "Invalid spooling encoding '{}', using default 'json+zstd'. Valid values: json, json+zstd, json+lz4",
                    encoding_str
                );
                self.session.spooling_encoding = Some("json+zstd".to_string());
            }
        }

        self
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    pub fn auth(mut self, s: Auth) -> Self {
        self.auth = Some(s);
        self
    }

    pub fn auth_http_insecure(mut self, ahi: bool) -> Self {
        self.auth_http_insecure = ahi;
        self
    }

    pub fn max_attempt(mut self, s: usize) -> Self {
        self.retry.max_retries = s;
        self
    }

    /// Set the full retry/backoff policy for transient failures.
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }

    pub fn ssl(mut self, ssl: Ssl) -> Self {
        self.ssl = Some(ssl);
        self
    }

    pub fn build(self) -> Result<Client> {
        let session = self.session.build()?;
        let retry = self.retry.clone();

        if (self.auth.is_some() && session.url.scheme() == "http") && !self.auth_http_insecure {
            return Err(Error::BasicAuthWithHttp);
        }

        let mut client_builder =
            reqwest::ClientBuilder::new().timeout(session.client_request_timeout);

        if self.no_verify {
            client_builder = client_builder.danger_accept_invalid_certs(true);
        }

        if let Some(ssl) = &self.ssl {
            if let Some(root) = &ssl.root_cert {
                client_builder = client_builder.add_root_certificate(root.0.clone());
            }
        }

        let client = client_builder.build()?;

        #[cfg(feature = "spooling")]
        let segment_fetcher = self.segment_fetcher.unwrap_or_else(|| {
            let mut fetcher = SegmentFetcher::new(client.clone());
            if let Some(max_concurrent) = self.max_concurrent_segments {
                fetcher = fetcher.with_max_concurrent(max_concurrent);
            }
            fetcher
        });

        let cli = Client {
            auth: self.auth,
            url: session.url.clone(),
            session: RwLock::new(session),
            client,
            retry,
            #[cfg(feature = "spooling")]
            segment_fetcher,
        };

        Ok(cli)
    }
}

fn add_prepare_header(mut builder: RequestBuilder, session: &Session) -> RequestBuilder {
    //FIXME : set trino user from jwt ?
    builder = builder.header(HEADER_USER, &session.user);
    // TODO: difference with session.source?
    builder = builder.header(USER_AGENT, "trino-rust-client");
    if session.compression_disabled {
        builder = builder.header(ACCEPT_ENCODING, "identity")
    }
    builder
}

fn add_session_header(mut builder: RequestBuilder, session: &Session) -> RequestBuilder {
    builder = add_prepare_header(builder, session);
    builder = builder.header(HEADER_SOURCE, &session.source);

    if let Some(v) = &session.trace_token {
        builder = builder.header(HEADER_TRACE_TOKEN, v);
    }

    if !session.client_tags.is_empty() {
        builder = builder.header(HEADER_CLIENT_TAGS, session.client_tags.by_ref().join(","));
    }

    if let Some(v) = &session.client_info {
        builder = builder.header(HEADER_CLIENT_INFO, v);
    }

    if let Some(v) = &session.catalog {
        builder = builder.header(HEADER_CATALOG, v);
    }

    if let Some(v) = &session.schema {
        builder = builder.header(HEADER_SCHEMA, v);
    }

    if let Some(v) = &session.path {
        builder = builder.header(HEADER_PATH, v);
    }
    if let Some(v) = &session.timezone {
        builder = builder.header(HEADER_TIME_ZONE, v.to_string())
    }
    // TODO: add locale
    builder = add_header_map(builder, HEADER_SESSION, &session.properties);
    builder = add_header_map(
        builder,
        HEADER_RESOURCE_ESTIMATE,
        &session.resource_estimates,
    );
    builder = add_header_map(
        builder,
        HEADER_ROLE,
        &session
            .roles
            .by_ref()
            .map_kv(|(k, v)| (k.to_string(), v.to_string())),
    );
    builder = add_header_map(builder, HEADER_EXTRA_CREDENTIAL, &session.extra_credentials);
    builder = add_header_map(
        builder,
        HEADER_PREPARED_STATEMENT,
        &session.prepared_statements,
    );
    builder = builder.header(HEADER_TRANSACTION, session.transaction_id.as_header_value());
    builder = builder.header(HEADER_CLIENT_CAPABILITIES, "PATH,PARAMETRIC_DATETIME");

    // Add spooling header when feature is enabled
    #[cfg(feature = "spooling")]
    {
        if let Some(encoding) = &session.spooling_encoding {
            builder = builder.header(HEADER_SPOOLING, encoding);
        }
    }

    builder
}

fn add_header_map<'a>(
    mut builder: RequestBuilder,
    header: &str,
    map: impl IntoIterator<Item = (&'a String, &'a String)>,
) -> RequestBuilder {
    for (k, v) in map {
        let kv = encode_kv(k, v);
        builder = builder.header(header, kv);
    }
    builder
}

macro_rules! set_header {
    ($session:expr, $header:expr, $resp:expr) => {
        set_header!($session, $header, $resp, |x: &str| Some(Some(
            x.to_string()
        )));
    };

    ($session:expr, $header:expr, $resp:expr, $from_str:expr) => {
        if let Some(v) = $resp.headers().get($header) {
            match v.to_str() {
                Ok(s) => {
                    if let Some(s) = $from_str(s) {
                        $session = s;
                    }
                }
                Err(e) => warn!("parse header {} failed, reason: {}", $header, e),
            }
        }
    };
}

macro_rules! clear_header {
    ($session:expr, $header:expr, $resp:expr) => {
        if let Some(_) = $resp.headers().get($header) {
            $session = Default::default();
        }
    };
}

macro_rules! set_header_map {
    ($session:expr, $header:expr, $resp:expr) => {
        set_header_map!($session, $header, $resp, |x: &str| Some(x.to_string()));
    };
    ($session:expr, $header:expr, $resp:expr, $from_str:expr) => {
        for v in $resp.headers().get_all($header) {
            if let Some((k, v)) = decode_kv_from_header(v) {
                if let Some(parsed) = $from_str(&v) {
                    $session.insert(k, parsed);
                } else {
                    warn!("parse header {} value '{}' failed, ignoring", $header, v)
                }
            } else {
                warn!("decode '{:?}' failed", v)
            }
        }
    };
}

macro_rules! clear_header_map {
    ($session:expr, $header:expr, $resp:expr) => {
        for v in $resp.headers().get_all($header) {
            match v.to_str() {
                Ok(s) => {
                    $session.remove(s);
                }
                Err(e) => warn!("parse header {} failed, reason: {}", $header, e),
            }
        }
    };
}

fn transient_status(code: &StatusCode) -> bool {
    matches!(
        *code,
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
    )
}

/// Retry predicate for **idempotent** requests (fetching result pages via
/// `GET nextUri`). Any transient failure is safe to retry: gateway/availability
/// responses (HTTP 502/503/504) and low-level connect/timeout errors. Query,
/// decode, protocol and other errors are terminal.
fn need_retry_fetch(e: &Error) -> bool {
    match e {
        Error::HttpError(e) => {
            e.is_timeout() || e.is_connect() || e.status().as_ref().is_some_and(transient_status)
        }
        Error::HttpNotOk(code, _) => transient_status(code),
        _ => false,
    }
}

/// Retry predicate for **query submission** (`POST /v1/statement`). Only retry
/// when the request was definitely NOT processed by the server, so a
/// non-idempotent statement (e.g. `INSERT`/`UPDATE`/DDL via [`Client::execute`])
/// is never submitted twice. A timeout — or a 502/504 from an intermediary — is
/// ambiguous (the query may already be running) and is treated as terminal.
fn need_retry_submit(e: &Error) -> bool {
    match e {
        // Connection was never established, so the request was not sent.
        Error::HttpError(e) => e.is_connect(),
        // 503 means the coordinator rejected the request without processing it.
        Error::HttpNotOk(code, _) => *code == StatusCode::SERVICE_UNAVAILABLE,
        _ => false,
    }
}

/// Everything needed to fire a best-effort query cancellation from
/// [`RowStream`]'s `Drop`, without borrowing the [`Client`].
struct CancelOnDrop {
    client: reqwest::Client,
    url: String,
    auth: Option<Auth>,
}

/// A lazy stream of query rows, with the result columns resolved up front.
///
/// Created by [`Client::stream`]. The result columns are available immediately
/// via [`RowStream::columns`]; rows are then produced lazily, page by page, by
/// the [`Stream`] implementation — the whole result set is never buffered in
/// memory.
///
/// `RowStream` is [`Unpin`], so it can be polled directly (e.g. with
/// [`StreamExt::next`](futures::StreamExt::next)) without `pin!`, and [`Send`],
/// so it can be held across `.await` inside a spawned task.
///
/// # Cancellation
/// Dropping a `RowStream` before it is exhausted best-effort cancels the query
/// on the Trino coordinator (a fire-and-forget `DELETE`), so early termination
/// (`take`, `break`, an error, a dropped task) does not leave the query running
/// server-side and holding coordinator resources. Cancellation is skipped once
/// the query has finished normally, and requires a Tokio runtime to be active
/// at drop time.
pub struct RowStream<'a, T> {
    columns: Vec<Column>,
    cancel: Option<CancelOnDrop>,
    // Entered on every poll so events emitted while streaming (page fetches,
    // segment downloads) carry the query_id — the span from `stream()` itself
    // would otherwise close as soon as the RowStream is handed back.
    span: tracing::Span,
    inner: Pin<Box<dyn Stream<Item = Result<T>> + Send + 'a>>,
}

impl<T> RowStream<'_, T> {
    /// The result columns (name, Trino type name and full type signature),
    /// resolved before the first row is produced.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
}

impl<T> Stream for RowStream<'_, T> {
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        let _enter = me.span.enter();
        let polled = me.inner.as_mut().poll_next(cx);
        if let Poll::Ready(None) = polled {
            // The query finished normally — there is nothing to cancel.
            me.cancel = None;
        }
        polled
    }
}

impl<T> Drop for RowStream<'_, T> {
    fn drop(&mut self) {
        let Some(cancel) = self.cancel.take() else {
            return;
        };
        // Fire-and-forget; only possible from within a running Tokio runtime.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let mut req = cancel.client.delete(&cancel.url);
                if let Some(auth) = &cancel.auth {
                    req = match auth {
                        Auth::Basic(u, p) => req.basic_auth(u, p.as_ref()),
                        Auth::Jwt(t) => req.bearer_auth(t),
                        // Full acquisition/polling lands in a later task; for now
                        // only attach a token if one is already cached.
                        Auth::OAuth2(state) => match state.cached_token() {
                            Some(t) => req.bearer_auth(t),
                            None => req,
                        },
                    };
                }
                let _ = req.send().await;
            });
        }
    }
}

impl Client {
    /// Execute `sql` and stream the resulting rows lazily, page by page, without
    /// buffering the whole result set in memory.
    ///
    /// Trino returns results as a chain of pages linked by `nextUri`. This method
    /// first drives the query far enough to resolve the result schema (so
    /// [`RowStream::columns`] is available up front), then hands back a
    /// [`RowStream`] that follows the remaining pages on demand, yielding each
    /// row as it is decoded. Prefer it over [`Client::get_all`] for large result
    /// sets.
    ///
    /// Both the Direct and (with the `spooling` feature) Spooled protocols are
    /// supported. With spooling, rows are still materialized one segment at a
    /// time rather than for the entire query, keeping peak memory bounded.
    ///
    /// Unlike [`Client::get_all`], this does not reject a query that mixes the
    /// Direct and Spooled protocols across pages; each page is decoded according
    /// to its own protocol.
    ///
    /// The returned stream borrows `self`, so it must not outlive the [`Client`].
    ///
    /// # Example
    /// ```no_run
    /// # use trino_rust_client::{client::ClientBuilder, Row};
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// use futures::StreamExt;
    ///
    /// let client = ClientBuilder::new("user", "localhost").port(8080).build()?;
    /// let mut rows = client.stream::<Row>("SELECT 1").await?;
    /// println!("columns: {:?}", rows.columns());
    /// while let Some(row) = rows.next().await {
    ///     let row = row?;
    ///     // use row
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn stream<'a, T>(&'a self, sql: impl Into<String>) -> Result<RowStream<'a, T>>
    where
        T: Trino + Send + 'static,
        for<'de> T: serde::Deserialize<'de>,
    {
        let sql = sql.into();

        // Prime the query until the schema is known: follow pages until one
        // carries `columns` (or the query finishes without any). Errors on these
        // early pages are surfaced eagerly.
        let mut res = self.get_retry::<T>(sql).await?;
        // Span stored on the RowStream and entered on each `poll_next`, so
        // events emitted while streaming carry the query_id. (Entering it here
        // across the priming `.await`s would be the guard-across-await
        // anti-pattern; priming emits little, so it is left unspanned.)
        let span = tracing::info_span!("query_stream", query_id = %res.id);
        loop {
            if let Some(error) = res.error.take() {
                return Err(error.into());
            }
            if res.columns.is_some() || res.data.is_some() {
                break;
            }
            match res.next_uri.clone() {
                Some(url) => res = self.get_next_retry::<T>(&url).await?,
                None => break,
            }
        }

        let columns = res.columns.clone().unwrap_or_default();

        // Capture what is needed to cancel the query on early drop, without
        // borrowing `self` (so the cancel can be spawned as a 'static task).
        let cancel = Some(CancelOnDrop {
            client: self.client.clone(),
            url: format!("{}v1/query/{}", self.url, res.id),
            auth: self.auth.clone(),
        });

        let inner = async_stream::try_stream! {
            // `res` already holds the first schema-bearing page (with its data,
            // if any); keep decoding from there.
            let mut res = res;
            // Track raw columns across pages so later spooled pages can be decoded.
            #[cfg(feature = "spooling")]
            let mut raw_columns: Option<Vec<Column>> = res.columns.clone();

            loop {
                if let Some(error) = res.error.take() {
                    Err(Error::from(error))?;
                }

                #[cfg(feature = "spooling")]
                if raw_columns.is_none() {
                    raw_columns = res.columns.clone();
                }

                if let Some(data) = res.data.take() {
                    match data {
                        QueryResultData::Direct(rows) => {
                            for row in rows {
                                yield row;
                            }
                        }
                        #[cfg(feature = "spooling")]
                        QueryResultData::Spooled(spooled) => {
                            let cols = raw_columns.clone().or_else(|| res.columns.clone());
                            let ds = self.fetch_spooled_data::<T>(spooled, cols).await?;
                            for row in ds.into_vec() {
                                yield row;
                            }
                        }
                        #[cfg(not(feature = "spooling"))]
                        QueryResultData::Spooled(_) => {
                            Err(Error::Protocol(
                                "Server sent spooled data but 'spooling' feature is not enabled. \
                                 Add features = [\"spooling\"] to your trino-rust-client dependency in Cargo.toml.".to_string(),
                            ))?;
                        }
                    }
                }

                match res.next_uri.take() {
                    Some(url) => {
                        res = self.get_next_retry::<T>(&url).await?;
                    }
                    None => break,
                }
            }
        };

        Ok(RowStream {
            columns,
            cancel,
            span,
            inner: Box::pin(inner),
        })
    }

    /// Run `sql` and return the whole result set as a [`DataSet`].
    ///
    /// The entire result is buffered in memory — for large results prefer
    /// [`stream`](Client::stream). `T` is a `#[derive(Trino)]` row struct, or
    /// [`Row`] for a dynamically-typed result.
    #[tracing::instrument(skip_all, fields(query_id = tracing::field::Empty))]
    pub async fn get_all<T>(&self, sql: impl Into<String>) -> Result<DataSet<T>>
    where
        T: Trino + 'static,
        for<'de> T: serde::Deserialize<'de> + serde::Serialize,
    {
        let res = self.get_retry(sql.into()).await?;
        tracing::Span::current().record("query_id", res.id.as_str());

        // Store columns from responses (used for Direct protocol DataSet construction)
        let mut columns = res.columns;

        match res.data {
            Some(QueryResultData::Direct(rows)) => {
                // Direct protocol: accumulate Vec<T>, convert to DataSet at the end
                let mut all_rows = rows;

                let mut next = res.next_uri;
                while let Some(url) = &next {
                    let mut res = self.get_next_retry(url).await?;
                    next = res.next_uri;

                    // Collect columns from any response that has them
                    if columns.is_none() {
                        columns = res.columns.take();
                    }

                    if let Some(error) = res.error {
                        return Err(error.into());
                    }

                    if let Some(data) = res.data {
                        match data {
                            QueryResultData::Direct(rows) => {
                                all_rows.extend(rows);
                            }
                            #[cfg(feature = "spooling")]
                            QueryResultData::Spooled(_) => {
                                return Err(Error::Protocol(
                                    "Cannot mix Direct and Spooled protocols in same query".to_string(),
                                ));
                            }
                            #[cfg(not(feature = "spooling"))]
                            QueryResultData::Spooled(_) => {
                                return Err(Error::Protocol(
                                    "Server sent spooled data but 'spooling' feature is not enabled. \
                                     Add features = [\"spooling\"] to your trino-rust-client dependency in Cargo.toml.".to_string(),
                                ));
                            }
                        }
                    }
                }

                build_dataset(all_rows, columns)
            }
            #[cfg(feature = "spooling")]
            Some(QueryResultData::Spooled(spooled)) => {
                let mut dataset = self
                    .fetch_spooled_data::<T>(spooled, columns.clone())
                    .await?;

                let mut next = res.next_uri;
                while let Some(url) = &next {
                    let mut res = self.get_next_retry::<T>(url).await?;
                    next = res.next_uri;

                    if columns.is_none() {
                        columns = res.columns.take();
                    }

                    if let Some(error) = res.error {
                        return Err(error.into());
                    }

                    if let Some(data) = res.data {
                        match data {
                            QueryResultData::Direct(_) => {
                                return Err(Error::Protocol(
                                    "Cannot mix Direct and Spooled protocols in same query".to_string(),
                                ));
                            }
                            QueryResultData::Spooled(spooled) => {
                                tracing::info!("🗄️  Received SPOOLED protocol data - fetching from S3/MinIO");
                                let cols_for_spooled = columns.clone().or_else(|| res.columns.take());
                                let next_dataset = self
                                    .fetch_spooled_data::<T>(spooled, cols_for_spooled)
                                    .await?;
                                dataset.merge(next_dataset);
                            }
                        }
                    }
                }

                Ok(dataset)
            }
            #[cfg(not(feature = "spooling"))]
            Some(QueryResultData::Spooled(_)) => {
                Err(Error::Protocol(
                    "Server sent spooled data but 'spooling' feature is not enabled. \
                     Add features = [\"spooling\"] to your trino-rust-client dependency in Cargo.toml.".to_string(),
                ))
            }
            None => {
                // No initial data, wait for next response to detect protocol
                let mut next = res.next_uri;
                let mut protocol_detected = false;
                let mut all_rows: Vec<T> = Vec::new();
                #[cfg(feature = "spooling")]
                let mut dataset: Option<DataSet<T>> = None;

                while let Some(url) = &next {
                    let mut res = self.get_next_retry::<T>(url).await?;
                    next = res.next_uri;

                    if columns.is_none() {
                        columns = res.columns.take();
                    }

                    if let Some(error) = res.error {
                        return Err(error.into());
                    }

                    if let Some(data) = res.data {
                        match data {
                            QueryResultData::Direct(rows) => {
                                if !protocol_detected {
                                    protocol_detected = true;
                                }
                                all_rows.extend(rows);
                            }
                            #[cfg(feature = "spooling")]
                            QueryResultData::Spooled(spooled) => {
                                if !protocol_detected {
                                    protocol_detected = true;
                                    let cols_for_spooled = columns.clone().or_else(|| res.columns.take());
                                    dataset = Some(self.fetch_spooled_data::<T>(spooled, cols_for_spooled).await?);
                                } else {
                                    let cols_for_spooled = columns.clone().or_else(|| res.columns.take());
                                    let next_dataset = self.fetch_spooled_data::<T>(spooled, cols_for_spooled).await?;
                                    if let Some(ref mut ds) = dataset {
                                        ds.merge(next_dataset);
                                    }
                                }
                            }
                            #[cfg(not(feature = "spooling"))]
                            QueryResultData::Spooled(_) => {
                                return Err(Error::Protocol(
                                    "Server sent spooled data but 'spooling' feature is not enabled. \
                                     Add features = [\"spooling\"] to your trino-rust-client dependency in Cargo.toml.".to_string(),
                                ));
                            }
                        }
                    }
                }

                #[cfg(feature = "spooling")]
                if let Some(ds) = dataset {
                    Ok(ds)
                } else {
                    build_dataset(all_rows, columns)
                }
                #[cfg(not(feature = "spooling"))]
                build_dataset(all_rows, columns)
            }
        }
    }

    #[cfg(feature = "spooling")]
    async fn fetch_spooled_data<T: Trino + 'static>(
        &self,
        spooled: SpooledData,
        columns: Option<Vec<crate::models::Column>>,
    ) -> Result<DataSet<T>> {
        let segment_bytes = self
            .segment_fetcher
            .fetch_segments(spooled.segments)
            .await?;

        let dataset = self.decode_segments::<T>(&spooled.encoding, segment_bytes, columns)?;

        Ok(dataset)
    }

    #[cfg(feature = "spooling")]
    fn decode_segments<T: Trino + 'static>(
        &self,
        encoding: &str,
        segment_bytes: Vec<Vec<u8>>,
        columns: Option<Vec<crate::models::Column>>,
    ) -> Result<DataSet<T>> {
        let cols = columns.ok_or_else(|| {
            Error::Protocol("Column metadata required for spooling protocol".to_string())
        })?;

        let mut all_rows: Vec<Vec<serde_json::Value>> = Vec::new();

        let encoding = SpoolingEncoding::try_from(encoding).map_err(|e| {
            Error::Decode(format!(
                "Failed to parse encoding: {}. Only 'json' based formats are supported.",
                e
            ))
        })?;

        for bytes in segment_bytes {
            let json_str = decompress_segment_bytes(&bytes, &encoding)?;

            let mut rows: Vec<Vec<serde_json::Value>> = serde_json::from_str(&json_str)
                .map_err(|e| Error::Decode(format!("Failed to parse segment JSON: {}", e)))?;

            all_rows.append(&mut rows);
        }

        let json_obj = serde_json::json!({
            "columns": cols,
            "data": all_rows
        });

        let dataset: DataSet<T> = serde_json::from_value(json_obj)
            .map_err(|e| Error::Decode(format!("Failed to deserialize DataSet: {}", e)))?;

        Ok(dataset)
    }

    /**
     * Execute a SQL statement and return the result.
     * If the TRINO query returns an error, the method returns an error of type `Error::Query`
     * @param sql The SQL statement to execute
     * @return [`Result<ExecuteResult>`]` The result of the execution
     * */
    #[tracing::instrument(skip_all, fields(query_id = tracing::field::Empty))]
    pub async fn execute(&self, sql: impl Into<String>) -> Result<ExecuteResult> {
        // try the sql first
        let res = self.get_retry::<Row>(sql.into()).await?;
        tracing::Span::current().record("query_id", res.id.as_str());

        let mut next = res.next_uri;
        let mut final_uri = next.clone();

        // Trino attempts several times to execute a query before marking it as failed.
        // At the end, retrieve the URL of the last request to get the result
        while let Some(url) = &next {
            let res = self.get_next_retry::<Row>(url).await?;

            let next_uri = res.next_uri;

            // If next_uri is not None, update final_uri
            if next_uri.is_some() {
                final_uri = next_uri.clone();
            }
            next = next_uri;
        }

        let url = final_uri.ok_or_else(|| {
            Error::InternalError("No next URI available for execution result".to_string())
        })?;

        // Parse the final URI to get TrinoRetryResult
        let result = self.try_get_retry_result(&url).await?;

        if let Some(error) = result.error {
            return Err(error.into());
        }

        Ok(ExecuteResult {
            output_uri: None,
            update_type: result.update_type,
            update_count: result.update_count,
        })
    }

    /// The transaction this client's session is currently bound to.
    pub async fn transaction_id(&self) -> TransactionId {
        self.session.read().await.transaction_id.clone()
    }

    /// Bind the session to a transaction.
    ///
    /// Normally unnecessary — [`begin_transaction`](Self::begin_transaction)
    /// captures the identifier Trino issues. Use this to adopt a transaction
    /// started elsewhere.
    pub async fn set_transaction_id(&self, id: TransactionId) {
        self.session.write().await.transaction_id = id;
    }

    /// Start a transaction.
    ///
    /// Issues `START TRANSACTION` and captures the identifier Trino returns, so
    /// statements issued afterwards on this client run inside the transaction
    /// until [`commit`](Self::commit) or [`rollback`](Self::rollback).
    ///
    /// # Concurrency
    ///
    /// A transaction is a property of the whole client, so treat a client as
    /// single-threaded for as long as one is open. Statements already in flight
    /// when the transaction starts do not join it, and statements issued
    /// concurrently from another task will run inside it whether or not that
    /// was intended.
    ///
    /// The nesting check below is best-effort, not atomic: the session lock is
    /// released before `START TRANSACTION` is sent (holding it would deadlock
    /// against the write lock taken when the response is processed). Two tasks
    /// calling this concurrently can therefore both pass the check and open two
    /// transactions, of which only the last is retained — the other is orphaned
    /// on the coordinator until it times out. Use a separate client per
    /// transaction if you need concurrency.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transaction`] if a transaction is already active —
    /// Trino does not support nested transactions.
    ///
    /// On failure the transaction may nevertheless have been started, since the
    /// identifier is captured before the statement finishes. Call
    /// [`rollback`](Self::rollback) to discard it; that also clears an
    /// identifier the coordinator has already expired.
    pub async fn begin_transaction(&self) -> Result<()> {
        // Bind the guard to a local: holding it across `execute` would deadlock
        // against the write lock `update_session` takes.
        let active = self.session.read().await.transaction_id.is_active();
        if active {
            return Err(Error::Transaction(
                "a transaction is already active; Trino does not support nested transactions"
                    .to_string(),
            ));
        }
        self.execute("START TRANSACTION").await?;
        Ok(())
    }

    /// Commit the active transaction.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transaction`] if no transaction is active.
    pub async fn commit(&self) -> Result<()> {
        self.end_transaction("COMMIT").await
    }

    /// Roll back the active transaction.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transaction`] if no transaction is active.
    pub async fn rollback(&self) -> Result<()> {
        self.end_transaction("ROLLBACK").await
    }

    /// Shared implementation of [`commit`](Self::commit) and
    /// [`rollback`](Self::rollback).
    ///
    /// Trino answers either with `X-Trino-Clear-Transaction-Id`, which
    /// `update_session` turns back into `TransactionId::NoTransaction`.
    async fn end_transaction(&self, statement: &str) -> Result<()> {
        let active = self.session.read().await.transaction_id.is_active();
        if !active {
            return Err(Error::Transaction(format!(
                "no active transaction to {}",
                statement.to_lowercase()
            )));
        }
        self.execute(statement).await?;
        Ok(())
    }

    async fn try_get_retry_result(&self, url: &str) -> Result<TrinoRetryResult> {
        let response = self.client.get(url).send().await?;

        let result = response.json::<TrinoRetryResult>().await?;

        Ok(result)
    }

    fn retry_policy(&self) -> ExponentialBuilder {
        self.retry.backoff()
    }

    async fn get_retry<T>(&self, sql: String) -> Result<QueryResult<T>>
    where
        T: Trino + 'static,
        for<'de> T: serde::Deserialize<'de>,
    {
        let result = || async { self.get::<T>(sql.clone()).await };

        // Submission is not idempotent — retry only when definitely not processed.
        result
            .retry(self.retry_policy())
            .when(need_retry_submit)
            .await
    }

    async fn get_next_retry<T>(&self, url: &str) -> Result<QueryResult<T>>
    where
        T: Trino + 'static,
        for<'de> T: serde::Deserialize<'de>,
    {
        let result = || async { self.get_next(url).await };

        // Page fetches are idempotent GETs — any transient failure is retryable.
        result
            .retry(self.retry_policy())
            .when(need_retry_fetch)
            .await
    }

    /// Submit `sql` and return the first result page.
    ///
    /// Low-level building block: the returned [`QueryResult`] may carry a
    /// `next_uri` that you must follow with [`get_next`](Client::get_next) to
    /// retrieve the rest. Most callers should use [`get_all`](Client::get_all)
    /// or [`stream`](Client::stream), which handle pagination.
    pub async fn get<T>(&self, sql: impl Into<String>) -> Result<QueryResult<T>>
    where
        T: Trino + 'static,
        for<'de> T: serde::Deserialize<'de>,
    {
        let req = self
            .client
            .post(format!("{}v1/statement", self.url))
            .body(sql.into());
        let req = {
            let session = self.session.read().await;
            add_session_header(req, &session)
        };

        let req = self.auth_req(req);
        self.send(req, StatusCode::OK, |resp| async {
            let text = resp.text().await?;

            let data: QueryResult<T> = serde_json::from_str(&text)
                .map_err(|e| Error::Decode(format!("Failed to parse response: {}", e)))?;
            Ok(data)
        })
        .await
    }

    /// Fetch the next result page from a `next_uri` returned by a previous
    /// [`get`](Client::get) / `get_next` call.
    pub async fn get_next<T>(&self, url: &str) -> Result<QueryResult<T>>
    where
        T: Trino + 'static,
        for<'de> T: serde::Deserialize<'de>,
    {
        let req = self.client.get(url);
        let req = {
            let session = self.session.read().await;
            add_prepare_header(req, &session)
        };

        let req = self.auth_req(req);
        self.send(req, StatusCode::OK, |resp| async {
            let text = resp.text().await?;
            let data: QueryResult<T> = serde_json::from_str(&text)
                .map_err(|e| Error::Decode(format!("Failed to parse response: {}", e)))?;
            Ok(data)
        })
        .await
    }

    /// Cancel a running query by its id, releasing its resources on the
    /// coordinator.
    pub async fn cancel(&self, query_id: &str) -> Result<()> {
        let url = format!("{}v1/query/{}", self.url, query_id);
        let req = self.client.delete(url);
        let req = {
            let session = self.session.read().await;
            add_prepare_header(req, &session)
        };

        let req = self.auth_req(req);
        self.send(req, StatusCode::NO_CONTENT, |_| async { Ok(()) })
            .await
    }

    fn auth_req(&self, req: RequestBuilder) -> RequestBuilder {
        if let Some(auth) = self.auth.as_ref() {
            match auth {
                Auth::Basic(u, p) => req.basic_auth(u, p.as_ref()),
                Auth::Jwt(t) => req.bearer_auth(t),
                // Full acquisition/polling lands in a later task; for now only
                // attach a token if one is already cached.
                Auth::OAuth2(state) => match state.cached_token() {
                    Some(t) => req.bearer_auth(t),
                    None => req,
                },
            }
        } else {
            req
        }
    }

    async fn send<R, F, Fut>(
        &self,
        req: RequestBuilder,
        expected_status: StatusCode,
        handle_response: F,
    ) -> Result<R>
    where
        F: FnOnce(Response) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        // Clone up front so an OAuth2 401 can be retried with a fresh token.
        // Bodies here are `String`s, so `try_clone` always succeeds.
        let retry_req = req.try_clone();
        let resp = req.send().await?;

        if resp.status() == StatusCode::UNAUTHORIZED {
            if let (Some(Auth::OAuth2(state)), Some(retry_req)) = (self.auth.as_ref(), retry_req) {
                if let Some(challenge) = resp
                    .headers()
                    .get(reqwest::header::WWW_AUTHENTICATE)
                    .and_then(|v| v.to_str().ok())
                    .and_then(crate::auth::parse_www_authenticate)
                {
                    self.acquire_oauth2_token(state, &challenge).await?;
                    let resp = self.auth_req(retry_req).send().await?;
                    return self
                        .finish_send(resp, expected_status, handle_response)
                        .await;
                }
            }
        }

        self.finish_send(resp, expected_status, handle_response)
            .await
    }

    /// Shared response-status handling (extracted so both the first and the
    /// retried OAuth2 request go through the same path).
    async fn finish_send<R, F, Fut>(
        &self,
        resp: Response,
        expected_status: StatusCode,
        handle_response: F,
    ) -> Result<R>
    where
        F: FnOnce(Response) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        let status = resp.status();
        if status != expected_status {
            let data = resp.text().await.unwrap_or("".to_string());
            Err(Error::HttpNotOk(status, data))
        } else {
            self.update_session(&resp).await;
            handle_response(resp).await
        }
    }

    /// Acquire an OAuth2 token under a single-flight lock: if another task
    /// already refreshed while we waited, reuse that token instead of opening a
    /// second browser.
    async fn acquire_oauth2_token(
        &self,
        state: &std::sync::Arc<crate::auth::OAuth2State>,
        challenge: &crate::auth::Challenge,
    ) -> Result<()> {
        let before = state.cached_token();
        let _guard = state.acquire.lock().await;
        // Someone else finished the flow while we waited for the lock.
        if state.cached_token() != before {
            return Ok(());
        }
        let token = crate::auth::run_flow(&self.client, state, challenge).await?;
        *state.token.write().unwrap() = Some(token);
        Ok(())
    }

    async fn update_session(&self, resp: &Response) {
        let mut session = self.session.write().await;

        set_header!(session.catalog, HEADER_SET_CATALOG, resp);
        set_header!(session.schema, HEADER_SET_SCHEMA, resp);
        set_header!(session.path, HEADER_SET_PATH, resp);

        set_header_map!(session.properties, HEADER_SET_SESSION, resp);
        clear_header_map!(session.properties, HEADER_CLEAR_SESSION, resp);

        set_header_map!(session.roles, HEADER_SET_ROLE, resp, SelectedRole::from_str);

        set_header_map!(session.prepared_statements, HEADER_ADDED_PREPARE, resp);
        clear_header_map!(
            session.prepared_statements,
            HEADER_DEALLOCATED_PREPARE,
            resp
        );

        if let Some(v) = resp.headers().get(HEADER_STARTED_TRANSACTION_ID) {
            match v.to_str() {
                Ok(s) => session.transaction_id = TransactionId::from_header_value(s),
                Err(e) => warn!(
                    "parse header {} failed, reason: {}",
                    HEADER_STARTED_TRANSACTION_ID, e
                ),
            }
        }
        clear_header!(session.transaction_id, HEADER_CLEAR_TRANSACTION_ID, resp);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////
// helper functions

fn encode_kv(k: &str, v: &str) -> String {
    url::form_urlencoded::Serializer::new(String::new())
        .append_pair(k, v)
        .finish()
}

fn decode_kv_from_header(input: &HeaderValue) -> Option<(String, String)> {
    let kvs = url::form_urlencoded::parse(input.as_bytes()).collect::<Vec<_>>();
    if kvs.is_empty() {
        None
    } else {
        Some((kvs[0].0.to_string(), kvs[0].1.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use http::StatusCode;
    use reqwest::header::HeaderValue;

    use super::*;
    use crate::client::{decode_kv_from_header, need_retry_fetch, need_retry_submit};
    use crate::error::Error;
    use crate::transaction::TransactionId;

    #[test]
    fn test_decode_kv_from_header_plus_sign_to_space() {
        let header_value = HeaderValue::from_static("statement=show+tables");
        let result = decode_kv_from_header(&header_value);
        assert!(result.is_some());
        let (key, value) = result.unwrap();
        assert_eq!(key, "statement");
        assert_eq!(value, "show tables");
    }

    #[test]
    fn test_decode_kv_from_header_percent_encoding() {
        let header_value = HeaderValue::from_static("statement=show%20tables");
        let result = decode_kv_from_header(&header_value);
        assert!(result.is_some());
        let (key, value) = result.unwrap();
        assert_eq!(key, "statement");
        assert_eq!(value, "show tables");
    }

    fn http_not_ok(code: StatusCode) -> Error {
        Error::HttpNotOk(code, String::new())
    }

    #[test]
    fn fetch_retries_all_transient_statuses() {
        // Idempotent page fetches retry every transient gateway/availability status.
        for code in [
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::GATEWAY_TIMEOUT,
        ] {
            assert!(need_retry_fetch(&http_not_ok(code)), "{code}");
        }
        // Client errors and non-transient 5xx fail fast.
        for code in [
            StatusCode::BAD_REQUEST,
            StatusCode::UNAUTHORIZED,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert!(!need_retry_fetch(&http_not_ok(code)), "{code}");
        }
        assert!(!need_retry_fetch(&Error::Protocol(
            "mixed protocols".into()
        )));
        assert!(!need_retry_fetch(&Error::InconsistentData));
    }

    #[test]
    fn submit_only_retries_definitely_unprocessed() {
        // Submission is non-idempotent: only 503 (rejected, not processed) is retried.
        assert!(need_retry_submit(&http_not_ok(
            StatusCode::SERVICE_UNAVAILABLE
        )));
        // 502/504 are ambiguous (a proxy may have forwarded the query) -> terminal.
        assert!(!need_retry_submit(&http_not_ok(StatusCode::BAD_GATEWAY)));
        assert!(!need_retry_submit(&http_not_ok(
            StatusCode::GATEWAY_TIMEOUT
        )));
        assert!(!need_retry_submit(&http_not_ok(
            StatusCode::INTERNAL_SERVER_ERROR
        )));
    }

    #[tokio::test]
    async fn transaction_id_defaults_to_no_transaction() {
        let client = ClientBuilder::new("user", "localhost").build().unwrap();
        assert_eq!(client.transaction_id().await, TransactionId::NoTransaction);
    }

    #[tokio::test]
    async fn set_transaction_id_is_observable() {
        let client = ClientBuilder::new("user", "localhost").build().unwrap();
        let id = TransactionId::Id("17cbc429-462a-4da3-9a06-02b6507d0d01".to_string());
        client.set_transaction_id(id.clone()).await;
        assert_eq!(client.transaction_id().await, id);
    }

    #[tokio::test]
    async fn begin_transaction_rejects_nesting() {
        let client = ClientBuilder::new("user", "localhost").build().unwrap();
        client
            .set_transaction_id(TransactionId::Id("abc".to_string()))
            .await;

        let err = client.begin_transaction().await.unwrap_err();
        assert!(
            matches!(err, Error::Transaction(_)),
            "expected Error::Transaction, got {err:?}"
        );
    }

    #[tokio::test]
    async fn commit_without_transaction_is_rejected() {
        let client = ClientBuilder::new("user", "localhost").build().unwrap();
        let err = client.commit().await.unwrap_err();
        assert!(
            matches!(err, Error::Transaction(_)),
            "expected Error::Transaction, got {err:?}"
        );
    }

    #[tokio::test]
    async fn rollback_without_transaction_is_rejected() {
        let client = ClientBuilder::new("user", "localhost").build().unwrap();
        let err = client.rollback().await.unwrap_err();
        assert!(
            matches!(err, Error::Transaction(_)),
            "expected Error::Transaction, got {err:?}"
        );
    }
}

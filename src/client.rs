use std::collections::{HashMap, HashSet};

use backon::ExponentialBuilder;
use backon::Retryable;
use http::header::{ACCEPT_ENCODING, USER_AGENT};
use http::StatusCode;
use iterable::*;
use log::*;
use reqwest::header::HeaderValue;
use reqwest::{RequestBuilder, Response, Url};
use tokio::sync::RwLock;
use tokio::time::Duration;

use crate::auth::Auth;
use crate::error::TrinoRetryResult;
use crate::error::{Error, Result};
use crate::header::*;
use crate::selected_role::SelectedRole;
use crate::session::{Session, SessionBuilder};
use crate::ssl::Ssl;
use crate::transaction::TransactionId;
use crate::{DataSet, QueryResult, Row, Trino};

// TODO:
// allow_redirects
// proxies
// cancel

pub struct Client {
    client: reqwest::Client,
    session: RwLock<Session>,
    auth: Option<Auth>,
    max_attempt: usize,
    url: Url,
}

pub struct ClientBuilder {
    session: SessionBuilder,
    auth: Option<Auth>,
    max_attempt: usize,
    ssl: Option<Ssl>,
    no_verify: bool,
}

#[derive(Debug)]
pub struct ExecuteResult {
    pub output_uri: Option<String>,
}

impl ClientBuilder {
    pub fn new(user: impl ToString, host: impl ToString) -> Self {
        let builder = SessionBuilder::new(user, host);
        Self {
            session: builder,
            auth: None,
            max_attempt: 3,
            ssl: None,
            no_verify: false,
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

    ////////////////////////////////////////////////////////////////////////////////////////////////

    pub fn auth(mut self, s: Auth) -> Self {
        self.auth = Some(s);
        self
    }

    pub fn max_attempt(mut self, s: usize) -> Self {
        self.max_attempt = s;
        self
    }

    pub fn ssl(mut self, ssl: Ssl) -> Self {
        self.ssl = Some(ssl);
        self
    }

    pub fn build(self) -> Result<Client> {
        let session = self.session.build()?;
        let max_attempt = self.max_attempt;

        info!("session url: {:?}", session.url);

        if self.auth.is_some() && session.url.scheme() == "http" {
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

        let cli = Client {
            auth: self.auth,
            url: session.url.clone(),
            session: RwLock::new(session),
            client: client_builder.build()?,
            max_attempt,
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
    builder = builder.header(HEADER_TRANSACTION, session.transaction_id.to_str());
    builder = builder.header(HEADER_CLIENT_CAPABILITIES, "PATH,PARAMETRIC_DATETIME");
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
                if let Some(v) = $from_str(&v) {
                    $session.insert(k, v);
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

fn need_retry(e: &Error) -> bool {
    match e {
        Error::HttpError(e) => e.status() == Some(StatusCode::SERVICE_UNAVAILABLE),
        Error::HttpNotOk(code, _) => code == &StatusCode::SERVICE_UNAVAILABLE,
        _ => false,
    }
}

impl Client {
    pub async fn get_all<T: Trino + 'static>(&self, sql: String) -> Result<DataSet<T>> {
        let res = self.get_retry(sql).await?;
        let mut ret = res.data_set;

        let mut next = res.next_uri;
        while let Some(url) = &next {
            let res = self.get_next_retry(url).await?;
            next = res.next_uri;

            if let Some(error) = res.error {
                if error.error_code == 4 {
                    return Err(Error::Forbidden {
                        message: error.message,
                    });
                }
            }

            if let Some(d) = res.data_set {
                match &mut ret {
                    Some(ret) => ret.merge(d),
                    None => ret = Some(d),
                }
            }
        }

        if let Some(d) = ret {
            Ok(d)
        } else {
            Err(Error::EmptyData)
        }
    }

    /**
     * Execute a SQL statement and return the result.
     * If the TRINO query returns an error, the method returns an error of type `Error::TrinoError`
     * @param sql The SQL statement to execute
     * @return Result<ExecuteResult> The result of the execution
     * */
    pub async fn execute(&self, sql: String) -> Result<ExecuteResult> {
        // try the sql first
        let res = self.get_retry::<Row>(sql).await?;

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

        Ok(ExecuteResult { output_uri: None })
    }

    async fn try_get_retry_result(&self, url: &str) -> Result<TrinoRetryResult> {
        let response = self.client.get(url).send().await?;

        let result = response.json::<TrinoRetryResult>().await?;

        Ok(result)
    }

    fn retry_policy(&self) -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_max_times(self.max_attempt)
            .with_max_delay(Duration::from_secs(2))
    }

    async fn get_retry<T: Trino + 'static>(&self, sql: String) -> Result<QueryResult<T>> {
        let result = || async { self.get::<T>(sql.clone()).await };

        result.retry(self.retry_policy()).when(need_retry).await
    }

    async fn get_next_retry<T: Trino + 'static>(&self, url: &str) -> Result<QueryResult<T>> {
        let result = || async { self.get_next(url).await };

        result.retry(self.retry_policy()).when(need_retry).await
    }

    pub async fn get<T: Trino + 'static>(&self, sql: String) -> Result<QueryResult<T>> {
        let req = self.client.post(self.url.clone()).body(sql);
        let req = {
            let session = self.session.read().await;
            add_session_header(req, &session)
        };

        let req = self.auth_req(req);
        self.send(req).await
    }

    pub async fn get_next<T: Trino + 'static>(&self, url: &str) -> Result<QueryResult<T>> {
        let req = self.client.get(url);
        let req = {
            let session = self.session.read().await;
            add_prepare_header(req, &session)
        };

        let req = self.auth_req(req);
        self.send(req).await
    }

    fn auth_req(&self, req: RequestBuilder) -> RequestBuilder {
        if let Some(auth) = self.auth.as_ref() {
            match auth {
                Auth::Basic(u, p) => req.basic_auth(u, p.as_ref()),
                Auth::Jwt(t) => req.bearer_auth(t),
            }
        } else {
            req
        }
    }

    async fn send<T: Trino + 'static>(&self, req: RequestBuilder) -> Result<QueryResult<T>> {
        let resp = req.send().await?;
        let status = resp.status();
        if status != StatusCode::OK {
            let data = resp.text().await.unwrap_or("".to_string());
            Err(Error::HttpNotOk(status, data))
        } else {
            self.update_session(&resp).await;
            let data = resp.json::<QueryResult<T>>().await?;
            Ok(data)
        }
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

        set_header!(
            session.transaction_id,
            HEADER_STARTED_TRANSACTION_ID,
            resp,
            TransactionId::from_str
        );
        clear_header!(session.transaction_id, HEADER_CLEAR_TRANSACTION_ID, resp);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////
// helper functions

fn encode_kv(k: &str, v: &str) -> String {
    format!("{}={}", k, urlencoding::encode(v))
}

fn decode_kv_from_header(input: &HeaderValue) -> Option<(String, String)> {
    let s = input.to_str().ok()?;
    let kv = s.split('=').collect::<Vec<_>>();
    if kv.len() != 2 {
        return None;
    }
    let k = kv[0].to_string();
    let v = urlencoding::decode(kv[1]).ok()?;
    Some((k, v.to_string()))
}

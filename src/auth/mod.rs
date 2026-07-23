use std::fmt;
use std::sync::Arc;
use std::time::Duration;

mod oauth2;
pub(crate) use oauth2::run_flow;
pub use oauth2::{
    parse_www_authenticate, BrowserRedirectHandler, Challenge, OAuth2State, RedirectHandler,
};

const DEFAULT_MAX_POLL_ATTEMPTS: usize = 10;
const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone)]
#[non_exhaustive]
pub enum Auth {
    Basic(String, Option<String>),
    Jwt(String),
    OAuth2(Arc<OAuth2State>),
}

impl Auth {
    pub fn new_basic(username: impl ToString, password: Option<impl ToString>) -> Auth {
        Auth::Basic(username.to_string(), password.map(|p| p.to_string()))
    }

    pub fn new_jwt(token: impl ToString) -> Auth {
        Auth::Jwt(token.to_string())
    }

    /// Interactive OAuth2 using the default browser handler.
    pub fn new_oauth2() -> Auth {
        Auth::new_oauth2_with_handler(Arc::new(BrowserRedirectHandler))
    }

    /// Interactive OAuth2 with a caller-supplied redirect handler.
    pub fn new_oauth2_with_handler(handler: Arc<dyn RedirectHandler>) -> Auth {
        Auth::OAuth2(Arc::new(OAuth2State::new(
            handler,
            DEFAULT_MAX_POLL_ATTEMPTS,
            DEFAULT_POLL_TIMEOUT,
        )))
    }

    /// Override the token-server poll settings. No-op for non-OAuth2 auth.
    pub fn with_poll(self, max_attempts: usize, timeout: Duration) -> Auth {
        match self {
            Auth::OAuth2(state) => Auth::OAuth2(Arc::new(OAuth2State::new(
                Arc::clone(&state.handler),
                max_attempts,
                timeout,
            ))),
            other => other,
        }
    }
}

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Auth::Basic(name, _) => f
                .debug_struct("BasicAuth")
                .field("username", name)
                .field("password", &"******")
                .finish(),

            Auth::Jwt(_) => f.debug_struct("JwtAuth").field("token", &"******").finish(),

            Auth::OAuth2(_) => f
                .debug_struct("OAuth2Auth")
                .field("token", &"******")
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oauth2_debug_redacts_token() {
        let auth = Auth::new_oauth2();
        if let Auth::OAuth2(state) = &auth {
            *state.token.write().unwrap() = Some("super-secret".to_string());
        }
        let dbg = format!("{auth:?}");
        assert!(!dbg.contains("super-secret"), "token leaked: {dbg}");
        assert!(dbg.contains("OAuth2Auth"));
    }
}

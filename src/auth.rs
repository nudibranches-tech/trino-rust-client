use std::fmt;

#[derive(Clone)]
pub enum Auth {
    Basic(String, Option<String>),
    Jwt(String),
}

impl Auth {
    pub fn new_basic(username: impl ToString, password: Option<impl ToString>) -> Auth {
        Auth::Basic(username.to_string(), password.map(|p| p.to_string()))
    }

    pub fn new_jwt(token: impl ToString) -> Auth {
        Auth::Jwt(token.to_string())
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

            Auth::Jwt(token) => f.debug_struct("JwtAuth").field("token", token).finish(),
        }
    }
}

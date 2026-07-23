/// The redirect + token endpoints extracted from a Trino `WWW-Authenticate`
/// Bearer challenge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Challenge {
    pub x_redirect_server: String,
    pub x_token_server: String,
}

/// Parse a Trino OAuth2 `WWW-Authenticate: Bearer ...` challenge.
///
/// Returns `None` when the header is not a Bearer challenge or lacks
/// `x_token_server` (the one field the flow cannot proceed without).
pub fn parse_www_authenticate(header: &str) -> Option<Challenge> {
    let trimmed = header.trim();
    // Must be a Bearer challenge. Use `get` (not indexing) so a non-ASCII or
    // malformed header degrades to `None` instead of panicking on a byte slice
    // that lands inside a multi-byte char.
    match trimmed.get(..6) {
        Some(prefix) if prefix.eq_ignore_ascii_case("bearer") => {}
        _ => return None,
    }

    let mut x_redirect_server = None;
    let mut x_token_server = None;

    for part in trimmed.split(',') {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        // The first key may arrive as `bearer x_redirect_server`; take the last
        // whitespace-separated token as the real key.
        let key = key.trim().rsplit(char::is_whitespace).next().unwrap_or("").trim();
        let value = value.trim().trim_matches('"');
        match key.to_ascii_lowercase().as_str() {
            "x_redirect_server" => x_redirect_server = Some(value.to_string()),
            "x_token_server" => x_token_server = Some(value.to_string()),
            _ => {}
        }
    }

    Some(Challenge {
        // x_redirect_server can legitimately be absent (already-authenticated
        // reuse); default to empty so the handler simply has nothing to open.
        x_redirect_server: x_redirect_server.unwrap_or_default(),
        x_token_server: x_token_server?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_standard_challenge() {
        let h = r#"Bearer x_redirect_server="https://c/oauth2/token/initiate/abc", x_token_server="https://c/oauth2/token/abc""#;
        let c = parse_www_authenticate(h).expect("should parse");
        assert_eq!(c.x_redirect_server, "https://c/oauth2/token/initiate/abc");
        assert_eq!(c.x_token_server, "https://c/oauth2/token/abc");
    }

    #[test]
    fn tolerates_bearer_prefixed_key_quirk() {
        // Naive splitting yields the first key as `bearer x_redirect_server`.
        let h = r#"Bearer x_redirect_server="https://c/i", x_token_server="https://c/t""#;
        let c = parse_www_authenticate(h).expect("should parse");
        assert_eq!(c.x_redirect_server, "https://c/i");
        assert_eq!(c.x_token_server, "https://c/t");
    }

    #[test]
    fn ignores_param_order_and_extra_params() {
        let h = r#"Bearer realm="trino", x_token_server="https://c/t", x_redirect_server="https://c/i""#;
        let c = parse_www_authenticate(h).expect("should parse");
        assert_eq!(c.x_token_server, "https://c/t");
        assert_eq!(c.x_redirect_server, "https://c/i");
    }

    #[test]
    fn none_when_no_token_server() {
        let h = r#"Bearer x_redirect_server="https://c/i""#;
        assert!(parse_www_authenticate(h).is_none());
    }

    #[test]
    fn none_when_not_bearer() {
        assert!(parse_www_authenticate(r#"Basic realm="trino""#).is_none());
    }

    #[test]
    fn none_on_non_ascii_header_without_panicking() {
        // A multi-byte char straddling byte index 6 must not panic the byte slice.
        assert!(parse_www_authenticate("aaaaaé x_token_server=\"https://c/t\"").is_none());
    }
}

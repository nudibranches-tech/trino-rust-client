# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://book.async.rs/overview/stability-guarantees.html).

## [Unreleased]
### Security
- Updated dependencies to remediate 13 RustSec advisories in transitive crates, including `aws-lc-sys` (X.509/PKCS7 validation bypasses, timing side-channel), `quinn-proto` (DoS, memory exhaustion), `rustls-webpki` (CRL/name-constraint validation, parsing panic), `bytes` (integer overflow) and `slab` (out-of-bounds, yanked) [#44](https://github.com/nudibranches-tech/trino-rust-client/pull/44)

### Changed
- Refreshed direct dependency versions (`backon`, `chrono`, `futures`, `http`, `log`, `regex`, `reqwest`, `serde_json`, `tokio`, `tracing-subscriber`, `uuid`) [#44](https://github.com/nudibranches-tech/trino-rust-client/pull/44)
- Replaced the unmaintained `dotenv` dev-dependency with the maintained `dotenvy` [#44](https://github.com/nudibranches-tech/trino-rust-client/pull/44)

### Removed
- **Breaking:** Removed the unused `Trino` feature [#40](https://github.com/nudibranches-tech/trino-rust-client/pull/40)
- **Breaking:** The optional `spooling` codec dependencies (`base64`, `zstd`, `lz4`, `flate2`) are no longer exposed as standalone public features; they are now declared via `dep:` and can only be enabled through the `spooling` feature [#40](https://github.com/nudibranches-tech/trino-rust-client/pull/40)

### Added
- Support configuring the name of the result field when using the `Trino` derive macro [#43](https://github.com/nudibranches-tech/trino-rust-client/pull/43)

## [0.9.3] - 2026-02-19
### Added
- `auth_http_insecure` option to allow authentication over HTTP [#34](https://github.com/nudibranches-tech/trino-rust-client/pull/34)

### Changed
- Boxed `reqwest::Error` and `QueryError` in error enum to reduce `Result` size [#32](https://github.com/nudibranches-tech/trino-rust-client/pull/32)
- Removed unnecessary `clippy::result_large_err` suppressions [#32](https://github.com/nudibranches-tech/trino-rust-client/pull/32)
- Masked JWT token in `Auth::Jwt` debug output [#34](https://github.com/nudibranches-tech/trino-rust-client/pull/34)
- Refreshed dependency versions [#35](https://github.com/nudibranches-tech/trino-rust-client/pull/35)

## [0.9.2] - 2025-11-20
### Changed
- Bumped trino-rust-client-macros to 0.7.1
- Integration tests are no longer published to crates.io

## [0.9.1] - 2025-11-20
### Added
- Spooling protocol support for efficient large result set handling [#26](https://github.com/nudibranches-tech/trino-rust-client/pull/26)
- New `spooling_encoding` configuration option (supports json+zstd, json+lz4, json+gzip)
- New `max_concurrent_segments` configuration for controlling concurrent downloads
- Spooling feature flag with compression support (zstd, lz4, gzip)

### Changed
- Bumped trino-rust-client-macros to 0.7.0

## [0.8.0] - 2025-01-XX
### Added
- Return execute result with operation type and counts [#24](https://github.com/nudibranches-tech/trino-rust-client/pull/24)

### Fixed
- Fixed prepared statement header decoding [#23](https://github.com/nudibranches-tech/trino-rust-client/pull/23)
- Ensure finished prepared statement's results are parsed accordingly [#22](https://github.com/nudibranches-tech/trino-rust-client/pull/22)

## [0.5.1] - 2023-10-19
- Make Client::get and some functions public [#29](https://github.com/nooberfsh/prusto/pull/29)

## [0.5.0] - 2023-02-27
- v0.5.0 can be used with stable rust.
- Add SSL root certificate support [#22](https://github.com/nooberfsh/prusto/pull/22)
- Provide a feature flag for running as presto client [#19](https://github.com/nooberfsh/prusto/pull/19)

## [0.4.0] - 2022-02-07
- Use `Rust 2021`

## [0.3.0] - 2021-05-26
- Use `Trino` protocol
- Add `execute` to `Client`
- Add more session properties
- Fix deserialization of `ClientTypeSignatureParameter`

## [0.2.0] - 2021-01-06
- Add `len`, `as_slice` methods to `DataSet<T>`
- Update `tokio` stack to 1.0
- Use `rustls` instead of `native-tls`

## [0.1.2] - 2020-10-30
-  Make `QueryError::error_location` optional

## [0.1.1] - 2020-10-09
- Add `'static` bound to key and value types of map like types

## [0.1.0] - 2020-10-01
- Initial release

[Unreleased]: https://github.com/nudibranches-tech/trino-rust-client/compare/v0.9.3...HEAD
[0.9.3]: https://github.com/nudibranches-tech/trino-rust-client/compare/v0.9.2...v0.9.3
[0.9.2]: https://github.com/nudibranches-tech/trino-rust-client/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/nudibranches-tech/trino-rust-client/compare/v0.8.0...v0.9.1
[0.8.0]: https://github.com/nudibranches-tech/trino-rust-client/compare/v0.5.1...v0.8.0
[0.5.1]: https://github.com/nooberfsh/prusto/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/nooberfsh/prusto/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/nooberfsh/prusto/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/nooberfsh/prusto/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/nooberfsh/prusto/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/nooberfsh/prusto/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/nooberfsh/prusto/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/nooberfsh/prusto/tree/v0.1.0

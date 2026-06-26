//! Configuration & URL resolution — ported from `getDBURL` / `initConfig` in
//! Go's `cmd/journio/root.go` + `cmd/journio/utils.go`.
//!
//! Resolution order (matches Go): `--db-url` flag → `database_url` in
//! `journio-config.yaml` → `JOURNIO_SYSTEM_DATABASE_URL` env var.

use std::path::PathBuf;

use serde::Deserialize;

/// Subset of `journio-config.yaml` that the CLI consumes — ported from Go's
/// `Config` (`cmd/journio/config.go`). YAML keys match Go's `mapstructure` tags
/// exactly: mixed snake_case (`name`, `database_url`) + camelCase
/// (`runtimeConfig`).
#[allow(dead_code)]
#[derive(Debug, Default, Clone, Deserialize)]
pub struct CliConfig {
    pub name: Option<String>,
    #[serde(rename = "database_url")]
    pub database_url: Option<String>,
    #[serde(default, rename = "runtimeConfig")]
    pub runtime_config: RuntimeConfig,
    #[serde(default)]
    pub database: DatabaseConfig,
}

#[allow(dead_code)]
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeConfig {
    #[serde(default)]
    pub start: Vec<String>,
}

#[derive(Debug, Default, Clone, Deserialize)]
pub struct DatabaseConfig {
    #[serde(default)]
    pub migrate: Vec<String>,
}

/// Load the config file if it exists at `path` (or `./journio-config.yaml` when
/// `path` is `None`). Returns `Ok(None)` when no file is found.
pub fn load_config(path: Option<&str>) -> Result<Option<(CliConfig, PathBuf)>, String> {
    let candidate = match path {
        Some(p) => PathBuf::from(p),
        None => PathBuf::from("journio-config.yaml"),
    };

    if !candidate.exists() {
        return Ok(None);
    }

    let contents = std::fs::read_to_string(&candidate)
        .map_err(|e| format!("failed to read config file {}: {e}", candidate.display()))?;
    let mut cfg: CliConfig =
        serde_yaml::from_str(&contents).map_err(|e| format!("failed to parse config: {e}"))?;

    // Expand environment variables in the database URL (Go does this for all
    // string keys; the URL is the only one the CLI reads).
    if let Some(url) = cfg.database_url.take() {
        cfg.database_url = Some(expand_env(&url));
    }

    Ok(Some((cfg, candidate)))
}

/// Resolve the database URL from flag → config → env, with a human-readable
/// error when none is set. Ported from `getDBURL`.
pub fn resolve_db_url(flag: Option<&str>, config: Option<&CliConfig>) -> Result<String, String> {
    if let Some(url) = flag.filter(|s| !s.is_empty()) {
        return Ok(url.to_string());
    }
    if let Some(cfg) = config {
        if let Some(url) = cfg.database_url.as_deref().filter(|s| !s.is_empty()) {
            return Ok(url.to_string());
        }
    }
    if let Ok(url) = std::env::var("JOURNIO_SYSTEM_DATABASE_URL") {
        if !url.is_empty() {
            return Ok(url);
        }
    }
    Err(
        "missing database URL: set it with --db-url, the database_url field in \
         journio-config.yaml, or the JOURNIO_SYSTEM_DATABASE_URL environment variable"
            .to_string(),
    )
}

/// Mask the password in a URL/key-value string for safe logging — ported from
/// Go's `maskPassword`.
pub fn mask_password(url: &str) -> String {
    // Try URL form first.
    if let Ok(parsed) = url::parse(url) {
        if parsed.scheme().starts_with("postgres") || parsed.scheme() == "crdb" {
            if parsed.password().is_some() {
                let username = parsed.username();
                let mut masked = parsed.clone();
                let _ = masked.set_password(Some("***"));
                // set_password percent-encodes; rebuild manually to match Go.
                let rebuilt = format!(
                    "{}://{}:***@{}{}",
                    parsed.scheme(),
                    username,
                    parsed.host_str().unwrap_or(""),
                    parsed.path()
                );
                let rebuilt = match parsed.query() {
                    Some(q) => format!("{rebuilt}?{q}"),
                    None => rebuilt,
                };
                let rebuilt = match parsed.fragment() {
                    Some(f) => format!("{rebuilt}#{f}"),
                    None => rebuilt,
                };
                return rebuilt;
            }
            return parsed.to_string();
        }
    }
    // Fall back to libpq key=value form.
    let re = regex_lite::Regex::new(r"(?i)password\s*=\s*[^\s]+").expect("valid regex");
    re.replace_all(url, "password=***").to_string()
}

// Lightweight env-var expansion (`$VAR` / `${VAR}`), ported from Go's
// `os.ExpandEnv`. Undefined vars expand to empty.
fn expand_env(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'{' {
                if let Some(end) = s[i + 2..].find('}') {
                    let name = &s[i + 2..i + 2 + end];
                    if let Ok(val) = std::env::var(name) {
                        out.push_str(&val);
                    }
                    i = i + 2 + end + 1;
                    continue;
                }
            }
            // bare $VAR
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
                end += 1;
            }
            if end > start {
                let name = &s[start..end];
                if let Ok(val) = std::env::var(name) {
                    out.push_str(&val);
                }
                i = end;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

mod url {
    // Minimal URL parser to avoid pulling a URL crate for password masking.
    pub struct Url {
        scheme: String,
        username: String,
        password: Option<String>,
        host: String,
        path: String,
        query: Option<String>,
        fragment: Option<String>,
    }

    impl Url {
        pub fn scheme(&self) -> &str {
            &self.scheme
        }
        pub fn username(&self) -> &str {
            &self.username
        }
        pub fn password(&self) -> Option<&str> {
            self.password.as_deref()
        }
        pub fn host_str(&self) -> Option<&str> {
            Some(&self.host)
        }
        pub fn path(&self) -> &str {
            &self.path
        }
        pub fn query(&self) -> Option<&str> {
            self.query.as_deref()
        }
        pub fn fragment(&self) -> Option<&str> {
            self.fragment.as_deref()
        }
        pub fn clone(&self) -> Self {
            Self {
                scheme: self.scheme.clone(),
                username: self.username.clone(),
                password: self.password.clone(),
                host: self.host.clone(),
                path: self.path.clone(),
                query: self.query.clone(),
                fragment: self.fragment.clone(),
            }
        }
        #[allow(dead_code)]
        pub fn set_password(&mut self, password: Option<&str>) -> Result<(), ()> {
            self.password = password.map(|p| p.to_string());
            Ok(())
        }
    }

    impl std::fmt::Display for Url {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}://", self.scheme)?;
            if !self.username.is_empty() {
                write!(f, "{}", self.username)?;
                if let Some(pw) = &self.password {
                    write!(f, ":{}", pw)?;
                }
                write!(f, "@")?;
            }
            write!(f, "{}{}", self.host, self.path)?;
            if let Some(q) = &self.query {
                write!(f, "?{}", q)?;
            }
            if let Some(fr) = &self.fragment {
                write!(f, "#{}", fr)?;
            }
            Ok(())
        }
    }

    pub fn parse(input: &str) -> Result<Url, ()> {
        let scheme_end = input.find("://").ok_or(())?;
        let scheme = input[..scheme_end].to_string();
        let rest = &input[scheme_end + 3..];

        // Split off fragment.
        let (rest, fragment) = match rest.find('#') {
            Some(i) => (&rest[..i], Some(rest[i + 1..].to_string())),
            None => (rest, None),
        };
        // Split off query.
        let (authority_path, query) = match rest.find('?') {
            Some(i) => (&rest[..i], Some(rest[i + 1..].to_string())),
            None => (rest, None),
        };

        let (userinfo, host_path) = match authority_path.find('@') {
            Some(i) => (&authority_path[..i], &authority_path[i + 1..]),
            None => ("", authority_path),
        };

        let (username, password) = match userinfo.find(':') {
            Some(i) => (
                userinfo[..i].to_string(),
                Some(userinfo[i + 1..].to_string()),
            ),
            None => (userinfo.to_string(), None),
        };

        // Split host from path — host ends at the first '/'.
        let (host, path) = match host_path.find('/') {
            Some(i) => (host_path[..i].to_string(), host_path[i..].to_string()),
            None => (host_path.to_string(), String::new()),
        };

        Ok(Url {
            scheme,
            username,
            password,
            host,
            path,
            query,
            fragment,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_password_replaces_password_in_url() {
        assert_eq!(
            mask_password("postgres://user:secret@host:5432/db"),
            "postgres://user:***@host:5432/db"
        );
    }

    #[test]
    fn mask_password_preserves_url_without_password() {
        assert_eq!(
            mask_password("postgres://host:5432/db"),
            "postgres://host:5432/db"
        );
    }

    #[test]
    fn mask_password_handles_keyvalue_form() {
        assert_eq!(
            mask_password("host=localhost password=secret user=foo"),
            "host=localhost password=*** user=foo"
        );
    }

    #[test]
    fn expand_env_replaces_bare_and_braced_vars() {
        unsafe {
            std::env::set_var("JOURNIO_TEST_VAR", "expanded");
        }
        assert_eq!(expand_env("$JOURNIO_TEST_VAR"), "expanded");
        assert_eq!(expand_env("${JOURNIO_TEST_VAR}!"), "expanded!");
        assert_eq!(
            expand_env("prefix-$JOURNIO_TEST_VAR-suffix"),
            "prefix-expanded-suffix"
        );
        unsafe {
            std::env::remove_var("JOURNIO_TEST_VAR");
        }
    }

    #[test]
    fn resolve_db_url_prefers_flag() {
        let cfg = CliConfig {
            database_url: Some("postgres://from-config".to_string()),
            ..Default::default()
        };
        assert_eq!(
            resolve_db_url(Some("postgres://from-flag"), Some(&cfg)).unwrap(),
            "postgres://from-flag"
        );
    }

    #[test]
    fn resolve_db_url_falls_back_to_config() {
        let cfg = CliConfig {
            database_url: Some("postgres://from-config".to_string()),
            ..Default::default()
        };
        assert_eq!(
            resolve_db_url(None, Some(&cfg)).unwrap(),
            "postgres://from-config"
        );
    }
}

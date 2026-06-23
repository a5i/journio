use journio_core::dialect::{Dialect, DialectName};

#[derive(Debug, Default, Clone, Copy)]
pub struct SqliteDialect;

impl Dialect for SqliteDialect {
    fn name(&self) -> DialectName {
        DialectName::Sqlite
    }

    fn schema_prefix(&self, _schema: &str) -> String {
        String::new()
    }

    fn rewrite_query(&self, query: &str) -> String {
        rewrite_postgres_placeholders(query)
    }

    fn lock_skip_locked(&self) -> &str {
        ""
    }

    fn lock_nowait(&self) -> &str {
        ""
    }

    fn supports_listen_notify(&self) -> bool {
        false
    }

    fn supports_array_parameters(&self) -> bool {
        false
    }

    fn supports_data_modifying_cte(&self) -> bool {
        true
    }
}

fn rewrite_postgres_placeholders(query: &str) -> String {
    let mut out = String::with_capacity(query.len());
    let bytes = query.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end > start {
                out.push('?');
                out.push_str(&query[start..end]);
                i = end;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::rewrite_postgres_placeholders;

    #[test]
    fn rewrites_numbered_placeholders() {
        let query = "UPDATE t SET a = $1, b = $2 WHERE id = $3 AND a = CAST($1 AS TEXT)";
        assert_eq!(
            rewrite_postgres_placeholders(query),
            "UPDATE t SET a = ?1, b = ?2 WHERE id = ?3 AND a = CAST(?1 AS TEXT)"
        );
    }
}

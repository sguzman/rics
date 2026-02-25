use crate::config::{FetchMode, LoadedSource, PaginationStrategy, resolve_path};
use anyhow::{Context, Result, bail};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, USER_AGENT};
use std::time::Duration;
use tracing::{debug, info, warn};
use url::Url;

#[derive(Debug, Clone)]
pub struct FetchedDocument {
    pub source_url: String,
    pub body: Vec<u8>,
    pub page_index: usize,
}

pub fn fetch_source_documents(source: &LoadedSource) -> Result<Vec<FetchedDocument>> {
    match source.config.fetch.mode {
        FetchMode::Http => fetch_http_documents(source),
        FetchMode::File => fetch_file_document(source),
        FetchMode::Inline => fetch_inline_document(source),
    }
}

fn fetch_http_documents(source: &LoadedSource) -> Result<Vec<FetchedDocument>> {
    let mut headers = HeaderMap::new();
    for (k, v) in &source.config.fetch.headers {
        let name = HeaderName::from_bytes(k.as_bytes())
            .with_context(|| format!("invalid header name {k}"))?;
        let value =
            HeaderValue::from_str(v).with_context(|| format!("invalid header value for {k}"))?;
        headers.insert(name, value);
    }

    if let Some(user_agent) = &source.config.fetch.user_agent {
        headers.insert(USER_AGENT, HeaderValue::from_str(user_agent)?);
    }

    let client = Client::builder()
        .timeout(Duration::from_secs(source.config.fetch.timeout_secs))
        .default_headers(headers)
        .build()
        .context("failed to build reqwest client")?;

    let base_url = source
        .config
        .fetch
        .base_url
        .as_ref()
        .context("fetch.base_url missing")?;

    if source.config.pagination.enabled
        && source.config.pagination.strategy == PaginationStrategy::NextLink
    {
        warn!(
            source = %source.config.source.key,
            "next_link pagination is declared but not fully implemented; using query-param style fallback"
        );
    }

    let mut docs = Vec::new();

    if source.config.pagination.enabled {
        let start = source.config.pagination.start_page;
        let end = start + source.config.pagination.max_pages;
        for (index, page) in (start..end).enumerate() {
            let page_url = build_paged_url(
                base_url,
                &source.config.pagination.page_param,
                page.to_string().as_str(),
            )?;
            let bytes = fetch_with_retries(
                &client,
                &source.config.fetch.method,
                &page_url,
                source.config.fetch.retry_attempts,
                source.config.fetch.retry_backoff_ms,
            )?;

            if bytes.is_empty() && source.config.pagination.stop_when_no_results {
                info!(
                    source = %source.config.source.key,
                    page,
                    "stopping pagination because response is empty"
                );
                break;
            }

            info!(
                source = %source.config.source.key,
                page,
                bytes = bytes.len(),
                url = %page_url,
                "fetched page"
            );

            docs.push(FetchedDocument {
                source_url: page_url,
                body: bytes,
                page_index: index,
            });
        }
    } else {
        let bytes = fetch_with_retries(
            &client,
            &source.config.fetch.method,
            base_url,
            source.config.fetch.retry_attempts,
            source.config.fetch.retry_backoff_ms,
        )?;
        docs.push(FetchedDocument {
            source_url: base_url.to_string(),
            body: bytes,
            page_index: 0,
        });
    }

    Ok(docs)
}

fn fetch_with_retries(
    client: &Client,
    method: &str,
    url: &str,
    retry_attempts: u8,
    retry_backoff_ms: u64,
) -> Result<Vec<u8>> {
    let attempts = retry_attempts.max(1);

    for attempt in 1..=attempts {
        let request = match method.to_ascii_uppercase().as_str() {
            "GET" => client.get(url),
            "POST" => client.post(url),
            other => bail!("unsupported fetch method {other}"),
        };

        match request.send() {
            Ok(resp) => {
                if !resp.status().is_success() {
                    let status = resp.status();
                    if attempt == attempts {
                        bail!("request to {url} failed with status {status}");
                    }
                    warn!(%url, %status, attempt, "request failed; retrying");
                } else {
                    return Ok(resp.bytes()?.to_vec());
                }
            }
            Err(err) => {
                if attempt == attempts {
                    return Err(err).with_context(|| format!("request to {url} failed"));
                }
                warn!(%url, attempt, error = %err, "request errored; retrying");
            }
        }

        std::thread::sleep(Duration::from_millis(retry_backoff_ms));
    }

    bail!("request to {url} failed after retries")
}

fn fetch_file_document(source: &LoadedSource) -> Result<Vec<FetchedDocument>> {
    let file_path = source
        .config
        .fetch
        .file_path
        .as_ref()
        .context("fetch.file_path missing for file mode")?;
    let resolved = resolve_path(&source.path, file_path)?;
    let bytes = std::fs::read(&resolved)
        .with_context(|| format!("failed to read file source {}", resolved.display()))?;

    info!(
        source = %source.config.source.key,
        file = %resolved.display(),
        bytes = bytes.len(),
        "loaded file source"
    );

    Ok(vec![FetchedDocument {
        source_url: format!("file://{}", resolved.display()),
        body: bytes,
        page_index: 0,
    }])
}

fn fetch_inline_document(source: &LoadedSource) -> Result<Vec<FetchedDocument>> {
    let inline = source
        .config
        .fetch
        .inline_data
        .as_ref()
        .context("fetch.inline_data missing for inline mode")?;

    debug!(
        source = %source.config.source.key,
        bytes = inline.len(),
        "loaded inline source"
    );

    Ok(vec![FetchedDocument {
        source_url: format!("inline://{}", source.config.source.key),
        body: inline.as_bytes().to_vec(),
        page_index: 0,
    }])
}

fn build_paged_url(base_url: &str, param: &str, page: &str) -> Result<String> {
    let mut url = Url::parse(base_url).with_context(|| format!("invalid base_url {base_url}"))?;

    let mut pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();

    let mut replaced = false;
    for (k, v) in &mut pairs {
        if k == param {
            *v = page.to_string();
            replaced = true;
            break;
        }
    }
    if !replaced {
        pairs.push((param.to_string(), page.to_string()));
    }

    {
        let mut qp = url.query_pairs_mut();
        qp.clear();
        for (k, v) in pairs {
            qp.append_pair(&k, &v);
        }
    }

    Ok(url.to_string())
}

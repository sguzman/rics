use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct LoadedSource {
    pub path: PathBuf,
    pub config: SourceConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    pub source: SourceMeta,
    #[serde(default)]
    pub fetch: FetchConfig,
    #[serde(default)]
    pub pagination: PaginationConfig,
    #[serde(default)]
    pub extract: ExtractConfig,
    #[serde(default)]
    pub map: BTreeMap<String, FieldRule>,
    #[serde(default)]
    pub date: DateConfig,
    #[serde(default)]
    pub event: EventConfig,
    #[serde(default)]
    pub pdf: PdfConfig,
    #[serde(default)]
    pub custom: CustomConfig,
    #[serde(default)]
    pub publish: PublishConfig,
}

impl SourceConfig {
    pub fn validate(&self) -> Result<()> {
        if self.source.key.trim().is_empty() {
            bail!("source.key must not be empty");
        }
        if self.source.name.trim().is_empty() {
            bail!("source.name must not be empty");
        }

        match self.fetch.mode {
            FetchMode::Http => {
                if self.fetch.base_url.is_none() {
                    bail!("fetch.base_url is required for http mode");
                }
            }
            FetchMode::File => {
                if self.fetch.file_path.is_none() {
                    bail!("fetch.file_path is required for file mode");
                }
            }
            FetchMode::Inline => {
                if self.fetch.inline_data.is_none() {
                    bail!("fetch.inline_data is required for inline mode");
                }
            }
        }

        if self.extract.format == ExtractFormat::Html
            && self.map.is_empty()
            && !(self.custom.enabled && self.custom.parser.is_some())
        {
            bail!("map section must not be empty for html extraction");
        }

        Ok(())
    }

    pub fn sanitized_source_dir_name(&self) -> String {
        sanitize_for_path(&self.source.key)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceMeta {
    pub key: String,
    pub name: String,
    pub domain: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default)]
    pub jurisdiction: Option<String>,
    #[serde(default)]
    pub default_country: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum FetchMode {
    #[default]
    Http,
    File,
    Inline,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FetchConfig {
    #[serde(default)]
    pub mode: FetchMode,
    #[serde(default = "default_get")]
    pub method: String,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub file_path: Option<PathBuf>,
    #[serde(default)]
    pub inline_data: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub template_vars: BTreeMap<String, String>,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "default_retry_attempts")]
    pub retry_attempts: u8,
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    #[serde(default)]
    pub user_agent: Option<String>,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            mode: FetchMode::Http,
            method: default_get(),
            base_url: None,
            file_path: None,
            inline_data: None,
            headers: BTreeMap::new(),
            template_vars: BTreeMap::new(),
            timeout_secs: default_timeout_secs(),
            retry_attempts: default_retry_attempts(),
            retry_backoff_ms: default_retry_backoff_ms(),
            user_agent: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum PaginationStrategy {
    #[default]
    QueryParam,
    NextLink,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PaginationConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub strategy: PaginationStrategy,
    #[serde(default = "default_page_param")]
    pub page_param: String,
    #[serde(default)]
    pub start_page: usize,
    #[serde(default = "default_max_pages")]
    pub max_pages: usize,
    #[serde(default = "default_true")]
    pub stop_when_no_results: bool,
    #[serde(default)]
    pub next_selector: Option<String>,
}

impl Default for PaginationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            strategy: PaginationStrategy::QueryParam,
            page_param: default_page_param(),
            start_page: 0,
            max_pages: default_max_pages(),
            stop_when_no_results: true,
            next_selector: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExtractFormat {
    #[default]
    Html,
    Json,
    PdfText,
    Text,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExtractConfig {
    #[serde(default)]
    pub format: ExtractFormat,
    #[serde(default)]
    pub root_selector: Option<String>,
    #[serde(default)]
    pub root_jsonpath: Option<String>,
    #[serde(default)]
    pub record_regex: Option<String>,
}

impl Default for ExtractConfig {
    fn default() -> Self {
        Self {
            format: ExtractFormat::Html,
            root_selector: None,
            root_jsonpath: None,
            record_regex: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct FieldRule {
    #[serde(default)]
    pub from: Option<String>,
    #[serde(rename = "const", default)]
    pub const_value: Option<String>,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub trim: bool,
    #[serde(default)]
    pub absolutize: bool,
    #[serde(default)]
    pub regex: Option<String>,
    #[serde(default)]
    pub capture: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DateConfig {
    #[serde(default = "default_primary_date")]
    pub primary: String,
    #[serde(default = "default_date_formats")]
    pub formats: Vec<String>,
    #[serde(default)]
    pub assume_timezone: Option<String>,
    #[serde(default = "default_true")]
    pub allow_month_only: bool,
    #[serde(default = "default_true")]
    pub allow_year_only: bool,
}

impl Default for DateConfig {
    fn default() -> Self {
        Self {
            primary: default_primary_date(),
            formats: default_date_formats(),
            assume_timezone: None,
            allow_month_only: true,
            allow_year_only: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct EventConfig {
    #[serde(default = "default_event_type")]
    pub event_type: String,
    #[serde(default)]
    pub subtype: Option<String>,
    #[serde(default = "default_status")]
    pub status: String,
    #[serde(default)]
    pub categories: Vec<String>,
    #[serde(default)]
    pub importance: Option<u8>,
}

impl Default for EventConfig {
    fn default() -> Self {
        Self {
            event_type: default_event_type(),
            subtype: None,
            status: default_status(),
            categories: Vec::new(),
            importance: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PdfConfig {
    #[serde(default)]
    pub page_range: Option<String>,
    #[serde(default = "default_true")]
    pub join_lines: bool,
    #[serde(default = "default_true")]
    pub normalize_whitespace: bool,
    #[serde(default)]
    pub record_split: Vec<PdfRecordSplit>,
    #[serde(default)]
    pub fields: BTreeMap<String, PdfFieldRule>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PdfRecordSplit {
    #[serde(default = "default_split_strategy")]
    pub strategy: String,
    pub pattern: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PdfFieldRule {
    pub pattern: String,
    #[serde(default = "default_capture")]
    pub capture: usize,
    #[serde(default)]
    pub optional: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct CustomConfig {
    #[serde(default)]
    pub parser: Option<String>,
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PublishConfig {
    #[serde(default)]
    pub mirror_dir: Option<PathBuf>,
    #[serde(default = "default_true")]
    pub mirror_source_subdir: bool,
    #[serde(default)]
    pub file_name_template: Option<String>,
}

pub fn load_sources_from_dir(config_dir: &Path) -> Result<Vec<LoadedSource>> {
    if !config_dir.exists() {
        bail!("config dir does not exist: {}", config_dir.display());
    }

    let mut loaded = Vec::new();
    for entry in WalkDir::new(config_dir) {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("toml") {
            continue;
        }

        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read source config: {}", path.display()))?;
        let config: SourceConfig = toml::from_str(&text)
            .with_context(|| format!("failed to parse toml in {}", path.display()))?;
        config
            .validate()
            .with_context(|| format!("invalid source config {}", path.display()))?;
        loaded.push(LoadedSource {
            path: path.to_path_buf(),
            config,
        });
    }

    loaded.sort_by(|a, b| a.config.source.key.cmp(&b.config.source.key));
    Ok(loaded)
}

pub fn load_source_file(config_path: &Path) -> Result<LoadedSource> {
    let text = std::fs::read_to_string(config_path)
        .with_context(|| format!("failed to read source config: {}", config_path.display()))?;
    let config: SourceConfig = toml::from_str(&text)
        .with_context(|| format!("failed to parse toml in {}", config_path.display()))?;
    config
        .validate()
        .with_context(|| format!("invalid source config {}", config_path.display()))?;
    Ok(LoadedSource {
        path: config_path.to_path_buf(),
        config,
    })
}

pub fn resolve_path(base_config_path: &Path, maybe_relative: &Path) -> Result<PathBuf> {
    if maybe_relative.is_absolute() {
        return Ok(maybe_relative.to_path_buf());
    }

    let parent = base_config_path.parent().ok_or_else(|| {
        anyhow!(
            "source config has no parent directory: {}",
            base_config_path.display()
        )
    })?;

    Ok(parent.join(maybe_relative))
}

pub fn sanitize_for_path(value: &str) -> String {
    value
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

fn default_true() -> bool {
    true
}

fn default_get() -> String {
    "GET".to_string()
}

fn default_timeout_secs() -> u64 {
    20
}

fn default_retry_attempts() -> u8 {
    2
}

fn default_retry_backoff_ms() -> u64 {
    500
}

fn default_page_param() -> String {
    "page".to_string()
}

fn default_max_pages() -> usize {
    1
}

fn default_primary_date() -> String {
    "date".to_string()
}

fn default_date_formats() -> Vec<String> {
    vec![
        "%Y-%m-%d".to_string(),
        "%Y/%m/%d".to_string(),
        "%B %d, %Y".to_string(),
        "%b %d, %Y".to_string(),
        "%B %Y".to_string(),
        "%b %Y".to_string(),
        "%Y".to_string(),
    ]
}

fn default_event_type() -> String {
    "event".to_string()
}

fn default_status() -> String {
    "scheduled".to_string()
}

fn default_split_strategy() -> String {
    "regex".to_string()
}

fn default_capture() -> usize {
    1
}

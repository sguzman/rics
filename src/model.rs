use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventTimeSpec {
    DateTime {
        start: DateTime<Utc>,
        end: Option<DateTime<Utc>>,
    },
    Date {
        start: NaiveDate,
        end: Option<NaiveDate>,
    },
    Month {
        year: i32,
        month: u32,
    },
    Quarter {
        year: i32,
        quarter: u8,
    },
    Year {
        year: i32,
    },
    Tbd {
        note: Option<String>,
    },
}

impl EventTimeSpec {
    pub fn year_bucket(&self) -> Option<i32> {
        match self {
            EventTimeSpec::DateTime { start, .. } => Some(start.year()),
            EventTimeSpec::Date { start, .. } => Some(start.year()),
            EventTimeSpec::Month { year, .. } => Some(*year),
            EventTimeSpec::Quarter { year, .. } => Some(*year),
            EventTimeSpec::Year { year } => Some(*year),
            EventTimeSpec::Tbd { .. } => None,
        }
    }

    pub fn start_date(&self) -> Option<NaiveDate> {
        match self {
            EventTimeSpec::DateTime { start, .. } => Some(start.date_naive()),
            EventTimeSpec::Date { start, .. } => Some(*start),
            EventTimeSpec::Month { year, month } => NaiveDate::from_ymd_opt(*year, *month, 1),
            EventTimeSpec::Quarter { year, quarter } => {
                let month = 1 + ((*quarter as u32).saturating_sub(1) * 3);
                NaiveDate::from_ymd_opt(*year, month, 1)
            }
            EventTimeSpec::Year { year } => NaiveDate::from_ymd_opt(*year, 1, 1),
            EventTimeSpec::Tbd { .. } => None,
        }
    }

    pub fn precision(&self) -> &'static str {
        match self {
            EventTimeSpec::DateTime { .. } => "datetime",
            EventTimeSpec::Date { .. } => "date",
            EventTimeSpec::Month { .. } => "month",
            EventTimeSpec::Quarter { .. } => "quarter",
            EventTimeSpec::Year { .. } => "year",
            EventTimeSpec::Tbd { .. } => "tbd",
        }
    }

    pub fn is_future_relative_to(&self, today: NaiveDate) -> bool {
        match self {
            EventTimeSpec::DateTime { start, .. } => start.date_naive() >= today,
            EventTimeSpec::Date { start, .. } => *start >= today,
            EventTimeSpec::Month { year, month } => {
                NaiveDate::from_ymd_opt(*year, *month, 1).is_some_and(|d| d >= today)
            }
            EventTimeSpec::Quarter { year, quarter } => {
                let month = 1 + ((*quarter as u32).saturating_sub(1) * 3);
                NaiveDate::from_ymd_opt(*year, month, 1).is_some_and(|d| d >= today)
            }
            EventTimeSpec::Year { year } => {
                NaiveDate::from_ymd_opt(*year, 1, 1).is_some_and(|d| d >= today)
            }
            EventTimeSpec::Tbd { .. } => true,
        }
    }

    pub fn end_date_exclusive(&self) -> Option<NaiveDate> {
        match self {
            EventTimeSpec::DateTime { end, .. } => end.map(|v| v.date_naive()),
            EventTimeSpec::Date { start, end } => Some(
                end.unwrap_or(*start)
                    .checked_add_signed(Duration::days(1))?,
            ),
            EventTimeSpec::Month { year, month } => {
                let (next_year, next_month) = if *month == 12 {
                    (*year + 1, 1)
                } else {
                    (*year, *month + 1)
                };
                NaiveDate::from_ymd_opt(next_year, next_month, 1)
            }
            EventTimeSpec::Quarter { year, quarter } => {
                let month = 1 + ((*quarter as u32).saturating_sub(1) * 3);
                let next_month = month + 3;
                if next_month > 12 {
                    NaiveDate::from_ymd_opt(*year + 1, next_month - 12, 1)
                } else {
                    NaiveDate::from_ymd_opt(*year, next_month, 1)
                }
            }
            EventTimeSpec::Year { year } => NaiveDate::from_ymd_opt(*year + 1, 1, 1),
            EventTimeSpec::Tbd { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidateEvent {
    pub source_key: String,
    pub source_name: String,
    pub source_event_id: Option<String>,
    pub source_url: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub time: EventTimeSpec,
    pub timezone: Option<String>,
    pub status: String,
    pub event_type: String,
    pub subtype: Option<String>,
    pub categories: Vec<String>,
    pub jurisdiction: Option<String>,
    pub country: Option<String>,
    pub importance: Option<u8>,
    pub confidence: Option<f32>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    pub uid: String,
    pub source_key: String,
    pub source_name: String,
    pub source_event_id: Option<String>,
    pub source_url: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub time: EventTimeSpec,
    pub timezone: Option<String>,
    pub status: String,
    pub event_type: String,
    pub subtype: Option<String>,
    pub categories: Vec<String>,
    pub jurisdiction: Option<String>,
    pub country: Option<String>,
    pub importance: Option<u8>,
    pub confidence: Option<f32>,
    pub metadata: BTreeMap<String, String>,
    pub sequence: u32,
    pub revision_hash: String,
    pub created_at: DateTime<Utc>,
    pub last_modified: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
}

impl EventRecord {
    pub fn year_bucket(&self) -> Option<i32> {
        self.time.year_bucket()
    }

    pub fn is_future_relative_to(&self, date: NaiveDate) -> bool {
        self.time.is_future_relative_to(date)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State {
    pub schema_version: u32,
    pub events: BTreeMap<String, EventRecord>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            schema_version: 1,
            events: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SourceRunReport {
    pub source_key: String,
    pub pages_fetched: usize,
    pub records_parsed: usize,
    pub inserted: usize,
    pub updated: usize,
    pub cancelled: usize,
    pub unchanged: usize,
}

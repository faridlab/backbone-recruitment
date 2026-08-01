use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "interview_status", rename_all = "snake_case")]
pub enum InterviewStatus {
    Scheduled,
    Completed,
    Cancelled,
    NoShow,
}

impl std::fmt::Display for InterviewStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scheduled => write!(f, "scheduled"),
            Self::Completed => write!(f, "completed"),
            Self::Cancelled => write!(f, "cancelled"),
            Self::NoShow => write!(f, "no_show"),
        }
    }
}

impl FromStr for InterviewStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "scheduled" => Ok(Self::Scheduled),
            "completed" => Ok(Self::Completed),
            "cancelled" => Ok(Self::Cancelled),
            "no_show" => Ok(Self::NoShow),
            _ => Err(format!("Unknown InterviewStatus variant: {}", s)),
        }
    }
}

impl Default for InterviewStatus {
    fn default() -> Self {
        Self::Scheduled
    }
}

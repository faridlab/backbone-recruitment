use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "candidate_source", rename_all = "snake_case")]
pub enum CandidateSource {
    Direct,
    Referral,
    JobBoard,
    Agency,
    WalkIn,
}

impl std::fmt::Display for CandidateSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Direct => write!(f, "direct"),
            Self::Referral => write!(f, "referral"),
            Self::JobBoard => write!(f, "job_board"),
            Self::Agency => write!(f, "agency"),
            Self::WalkIn => write!(f, "walk_in"),
        }
    }
}

impl FromStr for CandidateSource {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "direct" => Ok(Self::Direct),
            "referral" => Ok(Self::Referral),
            "job_board" => Ok(Self::JobBoard),
            "agency" => Ok(Self::Agency),
            "walk_in" => Ok(Self::WalkIn),
            _ => Err(format!("Unknown CandidateSource variant: {}", s)),
        }
    }
}

impl Default for CandidateSource {
    fn default() -> Self {
        Self::Direct
    }
}

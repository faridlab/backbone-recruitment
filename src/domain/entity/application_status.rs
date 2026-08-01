use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "application_status", rename_all = "snake_case")]
pub enum ApplicationStatus {
    Applied,
    Screening,
    Interview,
    Offer,
    Hired,
    Rejected,
}

impl std::fmt::Display for ApplicationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Applied => write!(f, "applied"),
            Self::Screening => write!(f, "screening"),
            Self::Interview => write!(f, "interview"),
            Self::Offer => write!(f, "offer"),
            Self::Hired => write!(f, "hired"),
            Self::Rejected => write!(f, "rejected"),
        }
    }
}

impl FromStr for ApplicationStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "applied" => Ok(Self::Applied),
            "screening" => Ok(Self::Screening),
            "interview" => Ok(Self::Interview),
            "offer" => Ok(Self::Offer),
            "hired" => Ok(Self::Hired),
            "rejected" => Ok(Self::Rejected),
            _ => Err(format!("Unknown ApplicationStatus variant: {}", s)),
        }
    }
}

impl Default for ApplicationStatus {
    fn default() -> Self {
        Self::Applied
    }
}

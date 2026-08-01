use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "requisition_status", rename_all = "snake_case")]
pub enum RequisitionStatus {
    Draft,
    Open,
    OnHold,
    Closed,
    Cancelled,
}

impl std::fmt::Display for RequisitionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Draft => write!(f, "draft"),
            Self::Open => write!(f, "open"),
            Self::OnHold => write!(f, "on_hold"),
            Self::Closed => write!(f, "closed"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl FromStr for RequisitionStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "draft" => Ok(Self::Draft),
            "open" => Ok(Self::Open),
            "on_hold" => Ok(Self::OnHold),
            "closed" => Ok(Self::Closed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(format!("Unknown RequisitionStatus variant: {}", s)),
        }
    }
}

impl Default for RequisitionStatus {
    fn default() -> Self {
        Self::Draft
    }
}

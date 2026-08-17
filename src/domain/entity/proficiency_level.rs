use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "proficiency_level", rename_all = "snake_case")]
pub enum ProficiencyLevel {
    Novice,
    Beginner,
    Intermediate,
    Advanced,
    Expert,
}

impl std::fmt::Display for ProficiencyLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Novice => write!(f, "novice"),
            Self::Beginner => write!(f, "beginner"),
            Self::Intermediate => write!(f, "intermediate"),
            Self::Advanced => write!(f, "advanced"),
            Self::Expert => write!(f, "expert"),
        }
    }
}

impl FromStr for ProficiencyLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "novice" => Ok(Self::Novice),
            "beginner" => Ok(Self::Beginner),
            "intermediate" => Ok(Self::Intermediate),
            "advanced" => Ok(Self::Advanced),
            "expert" => Ok(Self::Expert),
            _ => Err(format!("Unknown ProficiencyLevel variant: {}", s)),
        }
    }
}

impl Default for ProficiencyLevel {
    fn default() -> Self {
        Self::Intermediate
    }
}

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use super::InterviewStatus;
use super::AuditMetadata;

/// Strongly-typed ID for Interview
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct InterviewId(pub Uuid);

impl InterviewId {
    pub fn new(id: Uuid) -> Self { Self(id) }
    pub fn generate() -> Self { Self(Uuid::new_v4()) }
    pub fn into_inner(self) -> Uuid { self.0 }
}

impl std::fmt::Display for InterviewId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for InterviewId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for InterviewId {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl From<InterviewId> for Uuid {
    fn from(id: InterviewId) -> Self { id.0 }
}

impl AsRef<Uuid> for InterviewId {
    fn as_ref(&self) -> &Uuid { &self.0 }
}

impl std::ops::Deref for InterviewId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Interview {
    pub id: Uuid,
    pub company_id: Uuid,
    pub application_id: Uuid,
    pub interviewer_id: Uuid,
    pub scheduled_at: DateTime<Utc>,
    pub round: Option<i32>,
    pub interview_format: Option<String>,
    pub rating: Option<i32>,
    pub feedback: Option<String>,
    pub status: InterviewStatus,
    #[serde(default)]
    #[sqlx(json)]
    pub metadata: AuditMetadata,
}

impl Interview {
    /// Create a builder for Interview
    pub fn builder() -> InterviewBuilder {
        <InterviewBuilder as Default>::default()
    }

    /// Create a new Interview with required fields
    pub fn new(company_id: Uuid, application_id: Uuid, interviewer_id: Uuid, scheduled_at: DateTime<Utc>, status: InterviewStatus) -> Self {
        Self {
            id: Uuid::new_v4(),
            company_id,
            application_id,
            interviewer_id,
            scheduled_at,
            round: None,
            interview_format: None,
            rating: None,
            feedback: None,
            status,
            metadata: AuditMetadata::default(),
        }
    }

    /// Get the entity's unique identifier
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Get a strongly-typed ID for this entity
    pub fn typed_id(&self) -> InterviewId {
        InterviewId(self.id)
    }

    /// Get when this entity was created
    pub fn created_at(&self) -> Option<&DateTime<Utc>> {
        self.metadata.created_at.as_ref()
    }

    /// Get when this entity was last updated
    pub fn updated_at(&self) -> Option<&DateTime<Utc>> {
        self.metadata.updated_at.as_ref()
    }

    /// Check if this entity is soft deleted
    pub fn is_deleted(&self) -> bool {
        self.metadata.deleted_at.is_some()
    }

    /// Check if this entity is active (not deleted)
    pub fn is_active(&self) -> bool {
        self.metadata.deleted_at.is_none()
    }

    /// Get when this entity was deleted
    pub fn deleted_at(&self) -> Option<&DateTime<Utc>> {
        self.metadata.deleted_at.as_ref()
    }

    /// Get who created this entity
    pub fn created_by(&self) -> Option<&Uuid> {
        self.metadata.created_by.as_ref()
    }

    /// Get who last updated this entity
    pub fn updated_by(&self) -> Option<&Uuid> {
        self.metadata.updated_by.as_ref()
    }

    /// Get who deleted this entity
    pub fn deleted_by(&self) -> Option<&Uuid> {
        self.metadata.deleted_by.as_ref()
    }

    /// Get the current status
    pub fn status(&self) -> &InterviewStatus {
        &self.status
    }


    // ==========================================================
    // Fluent Setters (with_* for optional fields)
    // ==========================================================

    /// Set the round field (chainable)
    pub fn with_round(mut self, value: i32) -> Self {
        self.round = Some(value);
        self
    }

    /// Set the interview_format field (chainable)
    pub fn with_interview_format(mut self, value: String) -> Self {
        self.interview_format = Some(value);
        self
    }

    /// Set the rating field (chainable)
    pub fn with_rating(mut self, value: i32) -> Self {
        self.rating = Some(value);
        self
    }

    /// Set the feedback field (chainable)
    pub fn with_feedback(mut self, value: String) -> Self {
        self.feedback = Some(value);
        self
    }

    // ==========================================================
    // Partial Update
    // ==========================================================

    /// Apply partial updates from a map of field name to JSON value
    pub fn apply_patch(&mut self, fields: std::collections::HashMap<String, serde_json::Value>) {
        for (key, value) in fields {
            match key.as_str() {
                "company_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.company_id = v; }
                }
                "application_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.application_id = v; }
                }
                "interviewer_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.interviewer_id = v; }
                }
                "scheduled_at" => {
                    if let Ok(v) = serde_json::from_value(value) { self.scheduled_at = v; }
                }
                "round" => {
                    if let Ok(v) = serde_json::from_value(value) { self.round = v; }
                }
                "interview_format" => {
                    if let Ok(v) = serde_json::from_value(value) { self.interview_format = v; }
                }
                "rating" => {
                    if let Ok(v) = serde_json::from_value(value) { self.rating = v; }
                }
                "feedback" => {
                    if let Ok(v) = serde_json::from_value(value) { self.feedback = v; }
                }
                "status" => {
                    if let Ok(v) = serde_json::from_value(value) { self.status = v; }
                }
                _ => {} // ignore unknown fields
            }
        }
    }

    // <<< CUSTOM METHODS START >>>
    // <<< CUSTOM METHODS END >>>
}

impl super::Entity for Interview {
    type Id = Uuid;

    fn entity_id(&self) -> &Self::Id {
        &self.id
    }

    fn entity_type() -> &'static str {
        "Interview"
    }
}

impl backbone_core::PersistentEntity for Interview {
    fn entity_id(&self) -> String {
        self.id.to_string()
    }
    fn set_entity_id(&mut self, id: String) {
        if let Ok(uuid) = uuid::Uuid::parse_str(&id) {
            self.id = uuid;
        }
    }
    fn created_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.metadata.created_at
    }
    fn set_created_at(&mut self, ts: chrono::DateTime<chrono::Utc>) {
        self.metadata.created_at = Some(ts);
    }
    fn updated_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.metadata.updated_at
    }
    fn set_updated_at(&mut self, ts: chrono::DateTime<chrono::Utc>) {
        self.metadata.updated_at = Some(ts);
    }
    fn deleted_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.metadata.deleted_at
    }
    fn set_deleted_at(&mut self, ts: Option<chrono::DateTime<chrono::Utc>>) {
        self.metadata.deleted_at = ts;
    }
}

impl backbone_orm::EntityRepoMeta for Interview {
    fn column_types() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("id".to_string(), "uuid".to_string());
        m.insert("company_id".to_string(), "uuid".to_string());
        m.insert("application_id".to_string(), "uuid".to_string());
        m.insert("interviewer_id".to_string(), "uuid".to_string());
        m.insert("status".to_string(), "interview_status".to_string());
        m
    }
    fn search_fields() -> &'static [&'static str] {
        &[]
    }
    fn company_field() -> Option<&'static str> {
        Some("company_id")
    }
}

/// Builder for Interview entity
///
/// Provides a fluent API for constructing Interview instances.
/// System fields (id, metadata, timestamps) are auto-initialized.
#[derive(Debug, Clone, Default)]
pub struct InterviewBuilder {
    company_id: Option<Uuid>,
    application_id: Option<Uuid>,
    interviewer_id: Option<Uuid>,
    scheduled_at: Option<DateTime<Utc>>,
    round: Option<i32>,
    interview_format: Option<String>,
    rating: Option<i32>,
    feedback: Option<String>,
    status: Option<InterviewStatus>,
}

impl InterviewBuilder {
    /// Set the company_id field (required)
    pub fn company_id(mut self, value: Uuid) -> Self {
        self.company_id = Some(value);
        self
    }

    /// Set the application_id field (required)
    pub fn application_id(mut self, value: Uuid) -> Self {
        self.application_id = Some(value);
        self
    }

    /// Set the interviewer_id field (required)
    pub fn interviewer_id(mut self, value: Uuid) -> Self {
        self.interviewer_id = Some(value);
        self
    }

    /// Set the scheduled_at field (required)
    pub fn scheduled_at(mut self, value: DateTime<Utc>) -> Self {
        self.scheduled_at = Some(value);
        self
    }

    /// Set the round field (optional)
    pub fn round(mut self, value: i32) -> Self {
        self.round = Some(value);
        self
    }

    /// Set the interview_format field (optional)
    pub fn interview_format(mut self, value: String) -> Self {
        self.interview_format = Some(value);
        self
    }

    /// Set the rating field (optional)
    pub fn rating(mut self, value: i32) -> Self {
        self.rating = Some(value);
        self
    }

    /// Set the feedback field (optional)
    pub fn feedback(mut self, value: String) -> Self {
        self.feedback = Some(value);
        self
    }

    /// Set the status field (default: `InterviewStatus::default()`)
    pub fn status(mut self, value: InterviewStatus) -> Self {
        self.status = Some(value);
        self
    }

    /// Build the Interview entity
    ///
    /// Returns Err if any required field without a default is missing.
    pub fn build(self) -> Result<Interview, String> {
        let company_id = self.company_id.ok_or_else(|| "company_id is required".to_string())?;
        let application_id = self.application_id.ok_or_else(|| "application_id is required".to_string())?;
        let interviewer_id = self.interviewer_id.ok_or_else(|| "interviewer_id is required".to_string())?;
        let scheduled_at = self.scheduled_at.ok_or_else(|| "scheduled_at is required".to_string())?;

        Ok(Interview {
            id: Uuid::new_v4(),
            company_id,
            application_id,
            interviewer_id,
            scheduled_at,
            round: self.round,
            interview_format: self.interview_format,
            rating: self.rating,
            feedback: self.feedback,
            status: self.status.unwrap_or_default(),
            metadata: AuditMetadata::default(),
        })
    }
}

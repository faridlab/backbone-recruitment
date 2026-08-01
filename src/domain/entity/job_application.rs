use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use super::ApplicationStatus;
use super::AuditMetadata;

/// Strongly-typed ID for JobApplication
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobApplicationId(pub Uuid);

impl JobApplicationId {
    pub fn new(id: Uuid) -> Self { Self(id) }
    pub fn generate() -> Self { Self(Uuid::new_v4()) }
    pub fn into_inner(self) -> Uuid { self.0 }
}

impl std::fmt::Display for JobApplicationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for JobApplicationId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for JobApplicationId {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl From<JobApplicationId> for Uuid {
    fn from(id: JobApplicationId) -> Self { id.0 }
}

impl AsRef<Uuid> for JobApplicationId {
    fn as_ref(&self) -> &Uuid { &self.0 }
}

impl std::ops::Deref for JobApplicationId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct JobApplication {
    pub id: Uuid,
    pub company_id: Uuid,
    pub candidate_id: Uuid,
    pub requisition_id: Uuid,
    pub status: ApplicationStatus,
    pub applied_at: DateTime<Utc>,
    #[serde(default)]
    #[sqlx(json)]
    pub metadata: AuditMetadata,
}

impl JobApplication {
    /// Create a builder for JobApplication
    pub fn builder() -> JobApplicationBuilder {
        JobApplicationBuilder::default()
    }

    /// Create a new JobApplication with required fields
    pub fn new(company_id: Uuid, candidate_id: Uuid, requisition_id: Uuid, status: ApplicationStatus, applied_at: DateTime<Utc>) -> Self {
        Self {
            id: Uuid::new_v4(),
            company_id,
            candidate_id,
            requisition_id,
            status,
            applied_at,
            metadata: AuditMetadata::default(),
        }
    }

    /// Get the entity's unique identifier
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Get a strongly-typed ID for this entity
    pub fn typed_id(&self) -> JobApplicationId {
        JobApplicationId(self.id)
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
    pub fn status(&self) -> &ApplicationStatus {
        &self.status
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
                "candidate_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.candidate_id = v; }
                }
                "requisition_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.requisition_id = v; }
                }
                "status" => {
                    if let Ok(v) = serde_json::from_value(value) { self.status = v; }
                }
                "applied_at" => {
                    if let Ok(v) = serde_json::from_value(value) { self.applied_at = v; }
                }
                _ => {} // ignore unknown fields
            }
        }
    }

    // <<< CUSTOM METHODS START >>>
    // <<< CUSTOM METHODS END >>>
}

impl super::Entity for JobApplication {
    type Id = Uuid;

    fn entity_id(&self) -> &Self::Id {
        &self.id
    }

    fn entity_type() -> &'static str {
        "JobApplication"
    }
}

impl backbone_core::PersistentEntity for JobApplication {
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

impl backbone_orm::EntityRepoMeta for JobApplication {
    fn column_types() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("id".to_string(), "uuid".to_string());
        m.insert("company_id".to_string(), "uuid".to_string());
        m.insert("candidate_id".to_string(), "uuid".to_string());
        m.insert("requisition_id".to_string(), "uuid".to_string());
        m.insert("status".to_string(), "application_status".to_string());
        m
    }
    fn search_fields() -> &'static [&'static str] {
        &[]
    }
    fn company_field() -> Option<&'static str> {
        Some("company_id")
    }
}

/// Builder for JobApplication entity
///
/// Provides a fluent API for constructing JobApplication instances.
/// System fields (id, metadata, timestamps) are auto-initialized.
#[derive(Debug, Clone, Default)]
pub struct JobApplicationBuilder {
    company_id: Option<Uuid>,
    candidate_id: Option<Uuid>,
    requisition_id: Option<Uuid>,
    status: Option<ApplicationStatus>,
    applied_at: Option<DateTime<Utc>>,
}

impl JobApplicationBuilder {
    /// Set the company_id field (required)
    pub fn company_id(mut self, value: Uuid) -> Self {
        self.company_id = Some(value);
        self
    }

    /// Set the candidate_id field (required)
    pub fn candidate_id(mut self, value: Uuid) -> Self {
        self.candidate_id = Some(value);
        self
    }

    /// Set the requisition_id field (required)
    pub fn requisition_id(mut self, value: Uuid) -> Self {
        self.requisition_id = Some(value);
        self
    }

    /// Set the status field (default: `ApplicationStatus::default()`)
    pub fn status(mut self, value: ApplicationStatus) -> Self {
        self.status = Some(value);
        self
    }

    /// Set the applied_at field (default: `Utc::now()`)
    pub fn applied_at(mut self, value: DateTime<Utc>) -> Self {
        self.applied_at = Some(value);
        self
    }

    /// Build the JobApplication entity
    ///
    /// Returns Err if any required field without a default is missing.
    pub fn build(self) -> Result<JobApplication, String> {
        let company_id = self.company_id.ok_or_else(|| "company_id is required".to_string())?;
        let candidate_id = self.candidate_id.ok_or_else(|| "candidate_id is required".to_string())?;
        let requisition_id = self.requisition_id.ok_or_else(|| "requisition_id is required".to_string())?;

        Ok(JobApplication {
            id: Uuid::new_v4(),
            company_id,
            candidate_id,
            requisition_id,
            status: self.status.unwrap_or(ApplicationStatus::default()),
            applied_at: self.applied_at.unwrap_or(Utc::now()),
            metadata: AuditMetadata::default(),
        })
    }
}

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;
use rust_decimal::Decimal;

use super::OfferStatus;
use super::AuditMetadata;

/// Strongly-typed ID for JobOffer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobOfferId(pub Uuid);

impl JobOfferId {
    pub fn new(id: Uuid) -> Self { Self(id) }
    pub fn generate() -> Self { Self(Uuid::new_v4()) }
    pub fn into_inner(self) -> Uuid { self.0 }
}

impl std::fmt::Display for JobOfferId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for JobOfferId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for JobOfferId {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl From<JobOfferId> for Uuid {
    fn from(id: JobOfferId) -> Self { id.0 }
}

impl AsRef<Uuid> for JobOfferId {
    fn as_ref(&self) -> &Uuid { &self.0 }
}

impl std::ops::Deref for JobOfferId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct JobOffer {
    pub id: Uuid,
    pub company_id: Uuid,
    pub application_id: Uuid,
    pub proposed_salary: Option<Decimal>,
    pub employment_type: Option<String>,
    pub letter_template_id: Option<Uuid>,
    pub status: OfferStatus,
    pub offered_at: Option<DateTime<Utc>>,
    pub accepted_at: Option<DateTime<Utc>>,
    #[serde(default)]
    #[sqlx(json)]
    pub metadata: AuditMetadata,
}

impl JobOffer {
    /// Create a builder for JobOffer
    pub fn builder() -> JobOfferBuilder {
        <JobOfferBuilder as Default>::default()
    }

    /// Create a new JobOffer with required fields
    pub fn new(company_id: Uuid, application_id: Uuid, status: OfferStatus) -> Self {
        Self {
            id: Uuid::new_v4(),
            company_id,
            application_id,
            proposed_salary: None,
            employment_type: None,
            letter_template_id: None,
            status,
            offered_at: None,
            accepted_at: None,
            metadata: AuditMetadata::default(),
        }
    }

    /// Get the entity's unique identifier
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Get a strongly-typed ID for this entity
    pub fn typed_id(&self) -> JobOfferId {
        JobOfferId(self.id)
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
    pub fn status(&self) -> &OfferStatus {
        &self.status
    }


    // ==========================================================
    // Fluent Setters (with_* for optional fields)
    // ==========================================================

    /// Set the proposed_salary field (chainable)
    pub fn with_proposed_salary(mut self, value: Decimal) -> Self {
        self.proposed_salary = Some(value);
        self
    }

    /// Set the employment_type field (chainable)
    pub fn with_employment_type(mut self, value: String) -> Self {
        self.employment_type = Some(value);
        self
    }

    /// Set the letter_template_id field (chainable)
    pub fn with_letter_template_id(mut self, value: Uuid) -> Self {
        self.letter_template_id = Some(value);
        self
    }

    /// Set the offered_at field (chainable)
    pub fn with_offered_at(mut self, value: DateTime<Utc>) -> Self {
        self.offered_at = Some(value);
        self
    }

    /// Set the accepted_at field (chainable)
    pub fn with_accepted_at(mut self, value: DateTime<Utc>) -> Self {
        self.accepted_at = Some(value);
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
                "proposed_salary" => {
                    if let Ok(v) = serde_json::from_value(value) { self.proposed_salary = v; }
                }
                "employment_type" => {
                    if let Ok(v) = serde_json::from_value(value) { self.employment_type = v; }
                }
                "letter_template_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.letter_template_id = v; }
                }
                "status" => {
                    if let Ok(v) = serde_json::from_value(value) { self.status = v; }
                }
                "offered_at" => {
                    if let Ok(v) = serde_json::from_value(value) { self.offered_at = v; }
                }
                "accepted_at" => {
                    if let Ok(v) = serde_json::from_value(value) { self.accepted_at = v; }
                }
                _ => {} // ignore unknown fields
            }
        }
    }

    // <<< CUSTOM METHODS START >>>
    // <<< CUSTOM METHODS END >>>
}

impl super::Entity for JobOffer {
    type Id = Uuid;

    fn entity_id(&self) -> &Self::Id {
        &self.id
    }

    fn entity_type() -> &'static str {
        "JobOffer"
    }
}

impl backbone_core::PersistentEntity for JobOffer {
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

impl backbone_orm::EntityRepoMeta for JobOffer {
    fn column_types() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("id".to_string(), "uuid".to_string());
        m.insert("company_id".to_string(), "uuid".to_string());
        m.insert("application_id".to_string(), "uuid".to_string());
        m.insert("letter_template_id".to_string(), "uuid".to_string());
        m.insert("status".to_string(), "offer_status".to_string());
        m
    }
    fn search_fields() -> &'static [&'static str] {
        &[]
    }
    fn company_field() -> Option<&'static str> {
        Some("company_id")
    }
}

/// Builder for JobOffer entity
///
/// Provides a fluent API for constructing JobOffer instances.
/// System fields (id, metadata, timestamps) are auto-initialized.
#[derive(Debug, Clone, Default)]
pub struct JobOfferBuilder {
    company_id: Option<Uuid>,
    application_id: Option<Uuid>,
    proposed_salary: Option<Decimal>,
    employment_type: Option<String>,
    letter_template_id: Option<Uuid>,
    status: Option<OfferStatus>,
    offered_at: Option<DateTime<Utc>>,
    accepted_at: Option<DateTime<Utc>>,
}

impl JobOfferBuilder {
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

    /// Set the proposed_salary field (optional)
    pub fn proposed_salary(mut self, value: Decimal) -> Self {
        self.proposed_salary = Some(value);
        self
    }

    /// Set the employment_type field (optional)
    pub fn employment_type(mut self, value: String) -> Self {
        self.employment_type = Some(value);
        self
    }

    /// Set the letter_template_id field (optional)
    pub fn letter_template_id(mut self, value: Uuid) -> Self {
        self.letter_template_id = Some(value);
        self
    }

    /// Set the status field (default: `OfferStatus::default()`)
    pub fn status(mut self, value: OfferStatus) -> Self {
        self.status = Some(value);
        self
    }

    /// Set the offered_at field (optional)
    pub fn offered_at(mut self, value: DateTime<Utc>) -> Self {
        self.offered_at = Some(value);
        self
    }

    /// Set the accepted_at field (optional)
    pub fn accepted_at(mut self, value: DateTime<Utc>) -> Self {
        self.accepted_at = Some(value);
        self
    }

    /// Build the JobOffer entity
    ///
    /// Returns Err if any required field without a default is missing.
    pub fn build(self) -> Result<JobOffer, String> {
        let company_id = self.company_id.ok_or_else(|| "company_id is required".to_string())?;
        let application_id = self.application_id.ok_or_else(|| "application_id is required".to_string())?;

        Ok(JobOffer {
            id: Uuid::new_v4(),
            company_id,
            application_id,
            proposed_salary: self.proposed_salary,
            employment_type: self.employment_type,
            letter_template_id: self.letter_template_id,
            status: self.status.unwrap_or_default(),
            offered_at: self.offered_at,
            accepted_at: self.accepted_at,
            metadata: AuditMetadata::default(),
        })
    }
}

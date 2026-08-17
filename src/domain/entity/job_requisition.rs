use chrono::{DateTime, Utc, NaiveDate};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;
use rust_decimal::Decimal;

use super::RequisitionStatus;
use super::AuditMetadata;

/// Strongly-typed ID for JobRequisition
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobRequisitionId(pub Uuid);

impl JobRequisitionId {
    pub fn new(id: Uuid) -> Self { Self(id) }
    pub fn generate() -> Self { Self(Uuid::new_v4()) }
    pub fn into_inner(self) -> Uuid { self.0 }
}

impl std::fmt::Display for JobRequisitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for JobRequisitionId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for JobRequisitionId {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl From<JobRequisitionId> for Uuid {
    fn from(id: JobRequisitionId) -> Self { id.0 }
}

impl AsRef<Uuid> for JobRequisitionId {
    fn as_ref(&self) -> &Uuid { &self.0 }
}

impl std::ops::Deref for JobRequisitionId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct JobRequisition {
    pub id: Uuid,
    pub company_id: Uuid,
    pub department_id: Option<Uuid>,
    pub position_id: Option<Uuid>,
    pub title: String,
    pub headcount: i32,
    pub filled_headcount: i32,
    pub employment_type: Option<String>,
    pub status: RequisitionStatus,
    pub opened_by: Uuid,
    pub budget: Option<Decimal>,
    pub deadline: Option<NaiveDate>,
    #[serde(default)]
    #[sqlx(json)]
    pub metadata: AuditMetadata,
}

impl JobRequisition {
    /// Create a builder for JobRequisition
    pub fn builder() -> JobRequisitionBuilder {
        <JobRequisitionBuilder as Default>::default()
    }

    /// Create a new JobRequisition with required fields
    pub fn new(company_id: Uuid, title: String, headcount: i32, filled_headcount: i32, status: RequisitionStatus, opened_by: Uuid) -> Self {
        Self {
            id: Uuid::new_v4(),
            company_id,
            department_id: None,
            position_id: None,
            title,
            headcount,
            filled_headcount,
            employment_type: None,
            status,
            opened_by,
            budget: None,
            deadline: None,
            metadata: AuditMetadata::default(),
        }
    }

    /// Get the entity's unique identifier
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Get a strongly-typed ID for this entity
    pub fn typed_id(&self) -> JobRequisitionId {
        JobRequisitionId(self.id)
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
    pub fn status(&self) -> &RequisitionStatus {
        &self.status
    }


    // ==========================================================
    // Fluent Setters (with_* for optional fields)
    // ==========================================================

    /// Set the department_id field (chainable)
    pub fn with_department_id(mut self, value: Uuid) -> Self {
        self.department_id = Some(value);
        self
    }

    /// Set the position_id field (chainable)
    pub fn with_position_id(mut self, value: Uuid) -> Self {
        self.position_id = Some(value);
        self
    }

    /// Set the employment_type field (chainable)
    pub fn with_employment_type(mut self, value: String) -> Self {
        self.employment_type = Some(value);
        self
    }

    /// Set the budget field (chainable)
    pub fn with_budget(mut self, value: Decimal) -> Self {
        self.budget = Some(value);
        self
    }

    /// Set the deadline field (chainable)
    pub fn with_deadline(mut self, value: NaiveDate) -> Self {
        self.deadline = Some(value);
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
                "department_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.department_id = v; }
                }
                "position_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.position_id = v; }
                }
                "title" => {
                    if let Ok(v) = serde_json::from_value(value) { self.title = v; }
                }
                "headcount" => {
                    if let Ok(v) = serde_json::from_value(value) { self.headcount = v; }
                }
                "filled_headcount" => {
                    if let Ok(v) = serde_json::from_value(value) { self.filled_headcount = v; }
                }
                "employment_type" => {
                    if let Ok(v) = serde_json::from_value(value) { self.employment_type = v; }
                }
                "status" => {
                    if let Ok(v) = serde_json::from_value(value) { self.status = v; }
                }
                "opened_by" => {
                    if let Ok(v) = serde_json::from_value(value) { self.opened_by = v; }
                }
                "budget" => {
                    if let Ok(v) = serde_json::from_value(value) { self.budget = v; }
                }
                "deadline" => {
                    if let Ok(v) = serde_json::from_value(value) { self.deadline = v; }
                }
                _ => {} // ignore unknown fields
            }
        }
    }

    // <<< CUSTOM METHODS START >>>
    // <<< CUSTOM METHODS END >>>
}

impl super::Entity for JobRequisition {
    type Id = Uuid;

    fn entity_id(&self) -> &Self::Id {
        &self.id
    }

    fn entity_type() -> &'static str {
        "JobRequisition"
    }
}

impl backbone_core::PersistentEntity for JobRequisition {
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

impl backbone_orm::EntityRepoMeta for JobRequisition {
    fn column_types() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("id".to_string(), "uuid".to_string());
        m.insert("company_id".to_string(), "uuid".to_string());
        m.insert("department_id".to_string(), "uuid".to_string());
        m.insert("position_id".to_string(), "uuid".to_string());
        m.insert("status".to_string(), "requisition_status".to_string());
        m
    }
    fn search_fields() -> &'static [&'static str] {
        &["title"]
    }
    fn company_field() -> Option<&'static str> {
        Some("company_id")
    }
}

/// Builder for JobRequisition entity
///
/// Provides a fluent API for constructing JobRequisition instances.
/// System fields (id, metadata, timestamps) are auto-initialized.
#[derive(Debug, Clone, Default)]
pub struct JobRequisitionBuilder {
    company_id: Option<Uuid>,
    department_id: Option<Uuid>,
    position_id: Option<Uuid>,
    title: Option<String>,
    headcount: Option<i32>,
    filled_headcount: Option<i32>,
    employment_type: Option<String>,
    status: Option<RequisitionStatus>,
    opened_by: Option<Uuid>,
    budget: Option<Decimal>,
    deadline: Option<NaiveDate>,
}

impl JobRequisitionBuilder {
    /// Set the company_id field (required)
    pub fn company_id(mut self, value: Uuid) -> Self {
        self.company_id = Some(value);
        self
    }

    /// Set the department_id field (optional)
    pub fn department_id(mut self, value: Uuid) -> Self {
        self.department_id = Some(value);
        self
    }

    /// Set the position_id field (optional)
    pub fn position_id(mut self, value: Uuid) -> Self {
        self.position_id = Some(value);
        self
    }

    /// Set the title field (required)
    pub fn title(mut self, value: String) -> Self {
        self.title = Some(value);
        self
    }

    /// Set the headcount field (required)
    pub fn headcount(mut self, value: i32) -> Self {
        self.headcount = Some(value);
        self
    }

    /// Set the filled_headcount field (default: `0`)
    pub fn filled_headcount(mut self, value: i32) -> Self {
        self.filled_headcount = Some(value);
        self
    }

    /// Set the employment_type field (optional)
    pub fn employment_type(mut self, value: String) -> Self {
        self.employment_type = Some(value);
        self
    }

    /// Set the status field (default: `RequisitionStatus::default()`)
    pub fn status(mut self, value: RequisitionStatus) -> Self {
        self.status = Some(value);
        self
    }

    /// Set the opened_by field (required)
    pub fn opened_by(mut self, value: Uuid) -> Self {
        self.opened_by = Some(value);
        self
    }

    /// Set the budget field (optional)
    pub fn budget(mut self, value: Decimal) -> Self {
        self.budget = Some(value);
        self
    }

    /// Set the deadline field (optional)
    pub fn deadline(mut self, value: NaiveDate) -> Self {
        self.deadline = Some(value);
        self
    }

    /// Build the JobRequisition entity
    ///
    /// Returns Err if any required field without a default is missing.
    pub fn build(self) -> Result<JobRequisition, String> {
        let company_id = self.company_id.ok_or_else(|| "company_id is required".to_string())?;
        let title = self.title.ok_or_else(|| "title is required".to_string())?;
        let headcount = self.headcount.ok_or_else(|| "headcount is required".to_string())?;
        let opened_by = self.opened_by.ok_or_else(|| "opened_by is required".to_string())?;

        Ok(JobRequisition {
            id: Uuid::new_v4(),
            company_id,
            department_id: self.department_id,
            position_id: self.position_id,
            title,
            headcount,
            filled_headcount: self.filled_headcount.unwrap_or(0),
            employment_type: self.employment_type,
            status: self.status.unwrap_or_default(),
            opened_by,
            budget: self.budget,
            deadline: self.deadline,
            metadata: AuditMetadata::default(),
        })
    }
}

//! Requisition skill requirements (hand-authored, user-owned).
//!
//! A requisition can list the skills it wants, each with a minimum required
//! proficiency. Skill DEFINITIONS live in the learning module (`learning.
//! skills`) — this module stores only the reference, validated at write time
//! (fail-closed on an unknown skill id). The validation probe rides the
//! relayed ambient request scope, so a deployment whose composing service
//! decorated both modules' tables never sees another unit's skills; an
//! undecorated deployment accepts any existing skill id. That keeps the
//! modules decoupled at the Cargo level while the database-level reference
//! stays honest: no orphaned requirement rows pointing at skills the caller
//! cannot see.
//!
//! The set is replaced wholesale (one verb, one consistent list) and only
//! while the requisition is still editable — a requisition that is already
//! closed/cancelled is history, and its requirements must not drift.

use backbone_orm::org_scope;
use sqlx::PgPool;
use uuid::Uuid;

/// Errors from the requisition-skill write path.
#[derive(Debug, thiserror::Error)]
pub enum RequisitionSkillError {
    #[error("requisition {0} not found")]
    NotFound(Uuid),
    #[error("requisition {0} is not editable (requirements can only be set on draft/open requisitions)")]
    NotEditable(Uuid),
    #[error("skill {0} does not exist")]
    UnknownSkill(Uuid),
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

impl RequisitionSkillError {
    pub fn code(&self) -> &'static str {
        match self {
            RequisitionSkillError::NotFound(_) => "requisition_not_found",
            RequisitionSkillError::NotEditable(_) => "requisition_not_editable",
            RequisitionSkillError::UnknownSkill(_) => "unknown_skill",
            RequisitionSkillError::Db(_) => "internal_error",
        }
    }
    pub fn http_status(&self) -> u16 {
        match self {
            RequisitionSkillError::NotFound(_) => 404,
            RequisitionSkillError::NotEditable(_) | RequisitionSkillError::UnknownSkill(_) => 422,
            RequisitionSkillError::Db(_) => 500,
        }
    }
}

/// One required skill line.
#[derive(Debug, Clone)]
pub struct SkillRequirement {
    pub skill_id: Uuid,
    /// One of the proficiency scale values (novice … expert).
    pub required_proficiency: String,
}

/// The requisition-skill write path.
pub struct RequisitionSkillWriteService {
    pool: PgPool,
}

impl RequisitionSkillWriteService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Replace the skill set of a requisition in one call. Validates every
    /// skill exists (under the caller's org fence, when one is mounted),
    /// then swaps the rows in a single transaction (delete + insert), so a
    /// reader never sees a half-applied set.
    pub async fn set_skills(
        &self,
        requisition_id: Uuid,
        skills: Vec<SkillRequirement>,
    ) -> Result<(), RequisitionSkillError> {
        let mut tx = self.pool.begin().await?;
        // Relay the ambient request scope, when one is bound, onto this transaction:
        // a decorated deployment's row fence reads it; an unfenced one skips this.
        if let Some(scope) = org_scope::current_org_scope() {
            org_scope::bind_org_scope_on(&mut *tx, &scope).await?;
        }

        let status: Option<String> = sqlx::query_scalar(
            "SELECT status::text FROM recruitment.job_requisitions \
             WHERE id = $1 FOR UPDATE",
        )
        .bind(requisition_id)
        .fetch_optional(&mut *tx)
        .await?;
        match status.as_deref() {
            None => {
                tx.rollback().await?;
                return Err(RequisitionSkillError::NotFound(requisition_id));
            }
            Some("draft") | Some("open") => {}
            Some(_) => {
                tx.rollback().await?;
                return Err(RequisitionSkillError::NotEditable(requisition_id));
            }
        }

        // Validate the whole set BEFORE touching rows: one bad skill id leaves
        // the previous set fully intact. The probe carries no explicit tenant
        // predicate — the relayed scope fences the learning table on decorated
        // deployments, so a cross-unit skill id simply does not match.
        for s in &skills {
            let known: Option<Uuid> =
                sqlx::query_scalar("SELECT id FROM learning.skills WHERE id = $1")
                    .bind(s.skill_id)
                    .fetch_optional(&mut *tx)
                    .await?;
            if known.is_none() {
                tx.rollback().await?;
                return Err(RequisitionSkillError::UnknownSkill(s.skill_id));
            }
        }

        sqlx::query("DELETE FROM recruitment.requisition_skills WHERE requisition_id = $1")
            .bind(requisition_id)
            .execute(&mut *tx)
            .await?;

        for s in &skills {
            sqlx::query(
                r#"INSERT INTO recruitment.requisition_skills
                       (id, requisition_id, skill_id, required_proficiency, metadata)
                   VALUES ($1, $2, $3, $4::proficiency_level,
                           '{"created_at":null,"updated_at":null,"deleted_at":null,
                             "created_by":null,"updated_by":null,"deleted_by":null}'::jsonb)"#,
            )
            .bind(Uuid::new_v4())
            .bind(requisition_id)
            .bind(s.skill_id)
            .bind(&s.required_proficiency)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// The skill set of a requisition: `(skill_id, skill_name, required_
    /// proficiency)`, the name best-effort from learning (a skill renamed or
    /// archived away still shows its requirement line).
    pub async fn list_skills(
        &self,
        requisition_id: Uuid,
    ) -> Result<Vec<(Uuid, Option<String>, String)>, RequisitionSkillError> {
        let mut tx = self.pool.begin().await?;
        // Relay the ambient request scope, when one is bound, onto this transaction:
        // a decorated deployment's row fence reads it; an unfenced one skips this.
        if let Some(scope) = org_scope::current_org_scope() {
            org_scope::bind_org_scope_on(&mut *tx, &scope).await?;
        }

        let rows: Vec<(Uuid, Option<String>, String)> = sqlx::query_as(
            r#"SELECT rs.skill_id, s.name, rs.required_proficiency::text
                 FROM recruitment.requisition_skills rs
            LEFT JOIN learning.skills s ON s.id = rs.skill_id
                WHERE rs.requisition_id = $1
                ORDER BY s.name NULLS LAST, rs.skill_id"#,
        )
        .bind(requisition_id)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(rows)
    }
}

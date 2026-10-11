//! The module's extension (hand-written; ADR-0031): what the module adds beside its
//! generated services. The generated module and builder carry `ModuleExt` and
//! `ModuleBuilderExt` and dereference to them, so their fields read as `module.field`.

#[allow(unused_imports)]
use super::*;

/// State the module adds; read through the generated module's `Deref`.
pub struct ModuleExt {
    /// Hand-written write services, public so the composer / tests can drive the verbs directly.
    /// - offers: extend / hire (the hire-handoff producer: offer→accepted + `recruitment.hired`
    ///   outbox emit, atomically) / decline / withdraw, plus the offer-letter seam.
    /// - applications: the stage-driven transition engine (create / move_stage / refuse) owning
    ///   the requisition vacancy coupling.
    /// - interviews: schedule / complete / cancel, plus the interviewer-activity seam.
    /// - requisition skills: set / list, validated against the learning module's skills.
    pub job_offer_write_service: Arc<application::service::JobOfferWriteService>,
    pub job_application_write_service: Arc<application::service::JobApplicationWriteService>,
    /// The requisition open lifecycle (engine-gated draft→open) —
    /// user-owned, not schema-derived.
    pub requisition_lifecycle:
        Arc<application::service::requisition_lifecycle::RequisitionLifecycleService>,
    pub interview_write_service: Arc<application::service::InterviewWriteService>,
    pub requisition_skill_write_service: Arc<application::service::RequisitionSkillWriteService>,
}

/// State the builder adds.
pub struct ModuleBuilderExt {
    // optional outbound seams (default: unwired, fail-closed)
    activity_sink: Option<Arc<dyn application::service::ActivitySink>>,
    letter_sink: Option<Arc<dyn application::service::OfferLetterSink>>,
}

impl Default for ModuleBuilderExt {
    fn default() -> Self {
        Self {
            // optional outbound seams (default: unwired, fail-closed)
            activity_sink: None,
            letter_sink: None,
        }
    }
}

impl ModuleBuilderExt {
    /// Build the extension's state from what the generated build made.
    #[allow(unused_variables, clippy::redundant_clone)]
    pub(crate) fn build(self, parts: &ModuleParts<'_>) -> anyhow::Result<ModuleExt> {
        let db_pool = parts.db_pool.clone();
        let requisition_lifecycle = Arc::new(
            application::service::requisition_lifecycle::RequisitionLifecycleService::new(
                db_pool.clone(),
            ),
        );
        // Hand-written write services, bound to the same pool as the repos. Each opens its own
        // scoped transaction per verb; no shared mutable state, so an Arc is purely for cheap reuse.
        // Outbound seams default to unwired (fail-closed) unless the builder wired real adapters.
        let unwired_activity: Arc<dyn application::service::ActivitySink> =
            Arc::new(application::service::UnwiredActivitySink);
        let unwired_letters: Arc<dyn application::service::OfferLetterSink> =
            Arc::new(application::service::UnwiredOfferLetterSink);
        let activity_sink = self.activity_sink.clone().unwrap_or(unwired_activity);
        let letter_sink = self.letter_sink.clone().unwrap_or(unwired_letters);
        let job_offer_write_service = Arc::new(
            application::service::JobOfferWriteService::with_letter_sink(db_pool.clone(), letter_sink),
        );
        let job_application_write_service =
            Arc::new(application::service::JobApplicationWriteService::new(db_pool.clone()));
        let interview_write_service = Arc::new(
            application::service::InterviewWriteService::with_activity_sink(db_pool.clone(), activity_sink),
        );
        let requisition_skill_write_service =
            Arc::new(application::service::RequisitionSkillWriteService::new(db_pool.clone()));
        Ok(ModuleExt {
            job_offer_write_service,
            job_application_write_service,
            requisition_lifecycle,
            interview_write_service,
            requisition_skill_write_service,
        })
    }
}

impl crate::ThisModule {
    /// The guarded composition: read-only generic CRUD for every entity plus
    /// the validated verbs (stage moves, refusals, offer lifecycle, interview
    /// scheduling, skill sets). **Prefer this over `all_crud_routes()` for any
    /// real deployment** — generic writes on applications/offers/interviews
    /// would bypass the transition invariants.
    pub fn guarded_routes(&self) -> Router {
        presentation::http::guarded_routes::create_guarded_recruitment_routes(self)
    }
}

impl crate::ThisModuleBuilder {
    /// Wire a real activity adapter (interviewer notifications). Without one,
    /// a scheduling request that explicitly asks to notify fails closed.
    pub fn with_activity_sink(mut self, sink: Arc<dyn application::service::ActivitySink>) -> Self {
        self.activity_sink = Some(sink);
        self
    }
    /// Wire a real offer-letter adapter. Without one, extending an offer that
    /// references a letter template fails closed.
    pub fn with_letter_sink(mut self, sink: Arc<dyn application::service::OfferLetterSink>) -> Self {
        self.letter_sink = Some(sink);
        self
    }
}

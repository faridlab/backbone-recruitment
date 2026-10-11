// This directory's extension (hand-written; ADR-0031). The generated mod.rs (lib.rs for
// src/) pulls it in with include!, so its lines resolve against this directory.

// Hand-written write services (user-owned, never regenerated):
// - offer verbs + the hire-handoff producer (offer→accepted + `recruitment.hired`
//   outbox emit, in one tx) and the offer-letter seam;
// - stage-driven application transitions (the pipeline's one door, vacancy coupling);
// - interview scheduling verbs + the interviewer-activity seam;
// - requisition skill requirements (validated against the learning module's skills).
pub mod activity_port;
pub mod interview_write_service;
pub mod job_application_write_service;
pub mod job_offer_write_service;
pub mod recruitment_approvals_port;
pub mod requisition_lifecycle;
// The vacancy rule (openings = headcount − filled) the application and offer
// verbs refuse on before a candidate is put forward on a full requisition.
pub mod requisition_vacancy;
pub mod letter_port;
pub mod offer_letter_render;
pub mod requisition_skill_write_service;
pub use activity_port::{ActivityAck, ActivityCommand, ActivityRejected, ActivitySink, UnwiredActivitySink};
pub use interview_write_service::{InterviewError, InterviewWriteService, NewInterview};
pub use job_application_write_service::{
    ApplicationError, JobApplicationWriteService, NewJobApplication, PipelineStatus,
};
pub use job_offer_write_service::{
    ExtendOptions, JobOfferWriteService, NewJobOffer, OfferError, HIRED_EVENT_TYPE,
};
pub use letter_port::{LetterAck, LetterMessage, LetterRejected, OfferLetterSink, UnwiredOfferLetterSink};
pub use requisition_vacancy::{openings_left, NoOpenings};
pub use requisition_skill_write_service::{
    RequisitionSkillError, RequisitionSkillWriteService, SkillRequirement,
};

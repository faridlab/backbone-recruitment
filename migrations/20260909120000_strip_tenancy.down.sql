-- Hand-authored (user-owned). Not regenerated.
--
-- Best-effort restore sketch for the tenancy strip (ADR-0029). This is a breaking module
-- release against dev-stage databases: the down re-adds the company_id column as nullable
-- with its plain index and the company isolation policy shape, but restores NO data —
-- rows written after the strip (or after the decorator re-keyed them) carry org_unit_id
-- only. The composing service's tenancy decorator remains the live fence; treat this
-- down as a schema-shape sketch for archaeology, not a usable rollback.

ALTER TABLE recruitment.candidates             ADD COLUMN IF NOT EXISTS company_id uuid;
ALTER TABLE recruitment.interviews             ADD COLUMN IF NOT EXISTS company_id uuid;
ALTER TABLE recruitment.job_applications       ADD COLUMN IF NOT EXISTS company_id uuid;
ALTER TABLE recruitment.job_offers             ADD COLUMN IF NOT EXISTS company_id uuid;
ALTER TABLE recruitment.job_requisitions       ADD COLUMN IF NOT EXISTS company_id uuid;
ALTER TABLE recruitment.offer_letter_templates ADD COLUMN IF NOT EXISTS company_id uuid;
ALTER TABLE recruitment.recruitment_stages     ADD COLUMN IF NOT EXISTS company_id uuid;
ALTER TABLE recruitment.requisition_skills     ADD COLUMN IF NOT EXISTS company_id uuid;

CREATE INDEX IF NOT EXISTS idx_candidates_company_id             ON recruitment.candidates (company_id);
CREATE INDEX IF NOT EXISTS idx_interviews_company_id             ON recruitment.interviews (company_id);
CREATE INDEX IF NOT EXISTS idx_job_applications_company_id       ON recruitment.job_applications (company_id);
CREATE INDEX IF NOT EXISTS idx_job_offers_company_id             ON recruitment.job_offers (company_id);
CREATE INDEX IF NOT EXISTS idx_job_requisitions_company_id       ON recruitment.job_requisitions (company_id);
CREATE INDEX IF NOT EXISTS idx_offer_letter_templates_company_id ON recruitment.offer_letter_templates (company_id);
CREATE INDEX IF NOT EXISTS idx_recruitment_stages_company_id     ON recruitment.recruitment_stages (company_id);
CREATE INDEX IF NOT EXISTS idx_requisition_skills_company_id     ON recruitment.requisition_skills (company_id);

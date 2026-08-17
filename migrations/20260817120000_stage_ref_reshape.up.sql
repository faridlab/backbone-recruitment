-- Stage-driven application lifecycle reshape.
--
-- Why: the application pipeline is company configuration, not a fixed state
-- list. Applications now reference a configurable recruitment_stages row
-- (stage_id) instead of a hardcoded status enum, and the requisition tracks
-- how many of its openings are filled so remaining vacancies derive instead
-- of being stored twice. The refused marks (refused_at / refuse_reason) are
-- first-class because "refused" is the one closed state that is NOT a stage.
--
-- The module has no deployments yet, so job_applications is empty everywhere
-- this migration can run: stage_id can go straight to NOT NULL without a
-- backfill. If rows ever did exist, the SET NOT NULL below fails loudly
-- instead of silently mislabeling them.

-- 1) job_applications: stage columns replace the status enum.
ALTER TABLE recruitment.job_applications ADD COLUMN IF NOT EXISTS stage_id UUID;
ALTER TABLE recruitment.job_applications ADD COLUMN IF NOT EXISTS last_stage_id UUID;
ALTER TABLE recruitment.job_applications ADD COLUMN IF NOT EXISTS stage_updated_at TIMESTAMPTZ;
ALTER TABLE recruitment.job_applications ADD COLUMN IF NOT EXISTS date_closed TIMESTAMPTZ;
ALTER TABLE recruitment.job_applications ADD COLUMN IF NOT EXISTS refuse_reason TEXT;
ALTER TABLE recruitment.job_applications ADD COLUMN IF NOT EXISTS refused_at TIMESTAMPTZ;

ALTER TABLE recruitment.job_applications ALTER COLUMN stage_id SET NOT NULL;

DROP INDEX IF EXISTS recruitment.idx_job_applications_company_id_status;
CREATE INDEX IF NOT EXISTS idx_job_applications_company_id_stage_id
    ON recruitment.job_applications (company_id, stage_id);
CREATE INDEX IF NOT EXISTS idx_job_applications_last_stage_id
    ON recruitment.job_applications (last_stage_id);

ALTER TABLE recruitment.job_applications DROP COLUMN IF EXISTS status;
-- The type is referenced unqualified to match how the original migrations
-- created it (schema-blind DO block): the drop then resolves to the same type
-- under whatever search_path the runner used, instead of missing it.
DROP TYPE IF EXISTS application_status;

-- 2) job_requisitions: filled openings counter (remaining = headcount - filled).
ALTER TABLE recruitment.job_requisitions ADD COLUMN IF NOT EXISTS filled_headcount INTEGER NOT NULL DEFAULT 0;
ALTER TABLE recruitment.job_requisitions DROP CONSTRAINT IF EXISTS job_requisitions_filled_headcount_check;
ALTER TABLE recruitment.job_requisitions ADD CONSTRAINT job_requisitions_filled_headcount_check
    CHECK (filled_headcount >= 0);

-- 3) job_offers: optional letter template the extend verb renders from.
--    Logical reference only (the family convention): offer_letter_templates.id.
ALTER TABLE recruitment.job_offers ADD COLUMN IF NOT EXISTS letter_template_id UUID;

-- 4) Strict company fence for the tables introduced with this reshape:
--    recruitment_stages, requisition_skills, offer_letter_templates.
--    company_id is scoped per request via set_config('app.company_id', ...);
--    an unset var sees zero rows (fail-closed).
ALTER TABLE recruitment.recruitment_stages ENABLE ROW LEVEL SECURITY;
ALTER TABLE recruitment.recruitment_stages FORCE  ROW LEVEL SECURITY;
DROP POLICY IF EXISTS recruitment_stages_company_isolation ON recruitment.recruitment_stages;
CREATE POLICY recruitment_stages_company_isolation ON recruitment.recruitment_stages
    FOR ALL
    USING      (company_id = NULLIF(current_setting('app.company_id', true), '')::uuid)
    WITH CHECK (company_id = NULLIF(current_setting('app.company_id', true), '')::uuid);

ALTER TABLE recruitment.requisition_skills ENABLE ROW LEVEL SECURITY;
ALTER TABLE recruitment.requisition_skills FORCE  ROW LEVEL SECURITY;
DROP POLICY IF EXISTS requisition_skills_company_isolation ON recruitment.requisition_skills;
CREATE POLICY requisition_skills_company_isolation ON recruitment.requisition_skills
    FOR ALL
    USING      (company_id = NULLIF(current_setting('app.company_id', true), '')::uuid)
    WITH CHECK (company_id = NULLIF(current_setting('app.company_id', true), '')::uuid);

ALTER TABLE recruitment.offer_letter_templates ENABLE ROW LEVEL SECURITY;
ALTER TABLE recruitment.offer_letter_templates FORCE  ROW LEVEL SECURITY;
DROP POLICY IF EXISTS offer_letter_templates_company_isolation ON recruitment.offer_letter_templates;
CREATE POLICY offer_letter_templates_company_isolation ON recruitment.offer_letter_templates
    FOR ALL
    USING      (company_id = NULLIF(current_setting('app.company_id', true), '')::uuid)
    WITH CHECK (company_id = NULLIF(current_setting('app.company_id', true), '')::uuid);

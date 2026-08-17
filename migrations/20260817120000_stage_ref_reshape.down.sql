-- Revert the stage-driven reshape: restore the hardcoded status enum on
-- job_applications, drop the stage/refusal columns and the requisitions'
-- filled counter, and remove the fence policies this reshape installed.
-- Restored rows get the 'applied' status default — stage data has no enum
-- equivalent, so the revert is lossy by design (the reshape was never
-- deployed with data).

DROP POLICY IF EXISTS offer_letter_templates_company_isolation ON recruitment.offer_letter_templates;
ALTER TABLE recruitment.offer_letter_templates NO FORCE ROW LEVEL SECURITY;
ALTER TABLE recruitment.offer_letter_templates DISABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS requisition_skills_company_isolation ON recruitment.requisition_skills;
ALTER TABLE recruitment.requisition_skills NO FORCE ROW LEVEL SECURITY;
ALTER TABLE recruitment.requisition_skills DISABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS recruitment_stages_company_isolation ON recruitment.recruitment_stages;
ALTER TABLE recruitment.recruitment_stages NO FORCE ROW LEVEL SECURITY;
ALTER TABLE recruitment.recruitment_stages DISABLE ROW LEVEL SECURITY;

ALTER TABLE recruitment.job_requisitions DROP CONSTRAINT IF EXISTS job_requisitions_filled_headcount_check;
ALTER TABLE recruitment.job_requisitions DROP COLUMN IF EXISTS filled_headcount;

ALTER TABLE recruitment.job_offers DROP COLUMN IF EXISTS letter_template_id;

DROP INDEX IF EXISTS recruitment.idx_job_applications_last_stage_id;
DROP INDEX IF EXISTS recruitment.idx_job_applications_company_id_stage_id;

-- Recreated unqualified, mirroring the original migrations' schema-blind
-- creation, so the type lands where the runner's search_path originally put
-- it and the earlier migrations' own down files can drop it again cleanly.
DO $$ BEGIN
    CREATE TYPE application_status AS ENUM ('applied', 'screening', 'interview', 'offer', 'hired', 'rejected');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

ALTER TABLE recruitment.job_applications ADD COLUMN status application_status NOT NULL DEFAULT 'applied';
CREATE INDEX IF NOT EXISTS idx_job_applications_company_id_status ON recruitment.job_applications (company_id, status);

ALTER TABLE recruitment.job_applications DROP COLUMN IF EXISTS refused_at;
ALTER TABLE recruitment.job_applications DROP COLUMN IF EXISTS refuse_reason;
ALTER TABLE recruitment.job_applications DROP COLUMN IF EXISTS date_closed;
ALTER TABLE recruitment.job_applications DROP COLUMN IF EXISTS stage_updated_at;
ALTER TABLE recruitment.job_applications DROP COLUMN IF EXISTS last_stage_id;
ALTER TABLE recruitment.job_applications DROP COLUMN IF EXISTS stage_id;

-- Down: drop the declared foreign keys for recruitment module
ALTER TABLE IF EXISTS recruitment.requisition_skills DROP CONSTRAINT IF EXISTS fk_requisition_skills_requisition_id;
ALTER TABLE IF EXISTS recruitment.job_offers DROP CONSTRAINT IF EXISTS fk_job_offers_letter_template_id;
ALTER TABLE IF EXISTS recruitment.job_offers DROP CONSTRAINT IF EXISTS fk_job_offers_application_id;
ALTER TABLE IF EXISTS recruitment.job_applications DROP CONSTRAINT IF EXISTS fk_job_applications_requisition_id;
ALTER TABLE IF EXISTS recruitment.job_applications DROP CONSTRAINT IF EXISTS fk_job_applications_candidate_id;
ALTER TABLE IF EXISTS recruitment.job_applications DROP CONSTRAINT IF EXISTS fk_job_applications_stage_id;
ALTER TABLE IF EXISTS recruitment.interviews DROP CONSTRAINT IF EXISTS fk_interviews_application_id;

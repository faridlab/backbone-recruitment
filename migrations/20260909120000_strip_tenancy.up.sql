-- Hand-authored (user-owned). Not regenerated.
--
-- Strip every company-fence artifact from the recruitment tables (ADR-0029): the module is
-- tenant-agnostic; org scoping is installed by the COMPOSING service's tenancy decorator,
-- never by the module. Dropped here, per table: the company-leading indexes, the
-- <table>_company_isolation RLS policy, and the company_id column itself.
--
-- Ordering guard (the decorator must run FIRST on any database with data): the module
-- never moves tenancy data. A table is safe to strip when EITHER
--   a) it carries org_unit_id with no NULLs — the decorator backfilled it from company_id —
--      or b) it is empty (a fresh database: the earlier chain files created it empty).
-- Otherwise the strip RAISEs, naming the decorator step, rather than dropping a column
-- that still holds the only tenancy key. The file is re-runnable (every drop is IF EXISTS
-- and the tracker has no checksums), so a failed run retries cleanly after the decorator
-- lands.
--
-- RLS enable/force flags are deliberately NOT touched: the decorator owns those now.
-- No domain uniques are restored: recruitment was born under the company fence, so every
-- unique it ever carried included company_id — all of them are per-unit posture (candidate
-- email, stage name, template name, requisition skill) and are owned by the composing
-- service's tenancy decorator from here on.

DO $$
DECLARE
    t text;
    has_org boolean;
    org_nulls bigint;
    total bigint;
    offenders text := '';
BEGIN
    FOREACH t IN ARRAY ARRAY['candidates', 'interviews', 'job_applications', 'job_offers', 'job_requisitions', 'offer_letter_templates', 'recruitment_stages', 'requisition_skills']
    LOOP
        IF to_regclass(format('recruitment.%I', t)) IS NULL THEN
            CONTINUE; -- chain not fully applied on this database; nothing to strip
        END IF;

        SELECT EXISTS (
                   SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'recruitment' AND table_name = t AND column_name = 'org_unit_id'
               )
        INTO has_org;

        EXECUTE format('SELECT count(*) FROM recruitment.%I', t) INTO total;

        IF has_org THEN
            EXECUTE format(
                'SELECT count(*) FROM recruitment.%I WHERE org_unit_id IS NULL', t)
            INTO org_nulls;
        ELSE
            org_nulls := total; -- no org column: every row's only tenancy key is company_id
        END IF;

        IF has_org AND org_nulls = 0 THEN
            CONTINUE; -- decorator backfilled: safe
        END IF;
        IF total = 0 THEN
            CONTINUE; -- empty table (fresh database): safe
        END IF;
        offenders := offenders || format(' recruitment.%s (%s rows, %s rows not covered by org_unit_id);', t, total, org_nulls);
    END LOOP;

    IF offenders <> '' THEN
        RAISE EXCEPTION 'refusing to strip company_id — these tables are not yet covered by the tenancy decorator:%. Apply the composing service''s tenancy decorator (it backfills org_unit_id from company_id) and re-run; it is the only step that moves tenancy data.', offenders;
    END IF;
END $$;

-- ── candidates ─────────────────────────────────────────────────────────────────
DROP INDEX IF EXISTS recruitment.idx_candidates_company_id;
DROP INDEX IF EXISTS recruitment.idx_candidates_company_id_email;
DROP POLICY IF EXISTS candidates_company_isolation ON recruitment.candidates;
ALTER TABLE recruitment.candidates DROP COLUMN IF EXISTS company_id;

-- ── interviews ─────────────────────────────────────────────────────────────────
DROP INDEX IF EXISTS recruitment.idx_interviews_company_id_status;
DROP POLICY IF EXISTS interviews_company_isolation ON recruitment.interviews;
ALTER TABLE recruitment.interviews DROP COLUMN IF EXISTS company_id;

-- ── job_applications ───────────────────────────────────────────────────────────
-- idx_job_applications_company_id_status was already dropped by the stage_ref reshape;
-- the IF EXISTS keeps pre-reshape databases covered too.
DROP INDEX IF EXISTS recruitment.idx_job_applications_company_id_status;
DROP INDEX IF EXISTS recruitment.idx_job_applications_company_id_stage_id;
DROP POLICY IF EXISTS job_applications_company_isolation ON recruitment.job_applications;
ALTER TABLE recruitment.job_applications DROP COLUMN IF EXISTS company_id;

-- ── job_offers ─────────────────────────────────────────────────────────────────
DROP INDEX IF EXISTS recruitment.idx_job_offers_company_id_status;
DROP POLICY IF EXISTS job_offers_company_isolation ON recruitment.job_offers;
ALTER TABLE recruitment.job_offers DROP COLUMN IF EXISTS company_id;

-- ── job_requisitions ───────────────────────────────────────────────────────────
DROP INDEX IF EXISTS recruitment.idx_job_requisitions_company_id_status;
DROP POLICY IF EXISTS job_requisitions_company_isolation ON recruitment.job_requisitions;
ALTER TABLE recruitment.job_requisitions DROP COLUMN IF EXISTS company_id;

-- ── offer_letter_templates ─────────────────────────────────────────────────────
DROP INDEX IF EXISTS recruitment.idx_offer_letter_templates_company_id_name;
DROP POLICY IF EXISTS offer_letter_templates_company_isolation ON recruitment.offer_letter_templates;
ALTER TABLE recruitment.offer_letter_templates DROP COLUMN IF EXISTS company_id;

-- ── recruitment_stages ─────────────────────────────────────────────────────────
DROP INDEX IF EXISTS recruitment.idx_recruitment_stages_company_id_name;
DROP INDEX IF EXISTS recruitment.idx_recruitment_stages_company_id_sequence;
DROP POLICY IF EXISTS recruitment_stages_company_isolation ON recruitment.recruitment_stages;
ALTER TABLE recruitment.recruitment_stages DROP COLUMN IF EXISTS company_id;

-- ── requisition_skills ─────────────────────────────────────────────────────────
DROP INDEX IF EXISTS recruitment.idx_requisition_skills_company_id_requisition_id_skill_id;
DROP POLICY IF EXISTS requisition_skills_company_isolation ON recruitment.requisition_skills;
ALTER TABLE recruitment.requisition_skills DROP COLUMN IF EXISTS company_id;

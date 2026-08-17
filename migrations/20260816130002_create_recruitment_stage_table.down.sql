-- Down: drop recruitment.recruitment_stages table
DROP TABLE IF EXISTS recruitment.recruitment_stages CASCADE;
DROP FUNCTION IF EXISTS recruitment.recruitment_stages_audit_timestamp() CASCADE;

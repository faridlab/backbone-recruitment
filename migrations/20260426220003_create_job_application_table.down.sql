-- Down: drop recruitment.job_applications table
DROP TABLE IF EXISTS recruitment.job_applications CASCADE;
DROP FUNCTION IF EXISTS recruitment.job_applications_audit_timestamp() CASCADE;

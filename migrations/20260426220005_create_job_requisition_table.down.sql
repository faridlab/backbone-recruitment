-- Down: drop recruitment.job_requisitions table
DROP TABLE IF EXISTS recruitment.job_requisitions CASCADE;
DROP FUNCTION IF EXISTS recruitment.job_requisitions_audit_timestamp() CASCADE;

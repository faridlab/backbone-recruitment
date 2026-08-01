-- Down: drop recruitment.job_offers table
DROP TABLE IF EXISTS recruitment.job_offers CASCADE;
DROP FUNCTION IF EXISTS recruitment.job_offers_audit_timestamp() CASCADE;

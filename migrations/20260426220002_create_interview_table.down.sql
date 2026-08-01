-- Down: drop recruitment.interviews table
DROP TABLE IF EXISTS recruitment.interviews CASCADE;
DROP FUNCTION IF EXISTS recruitment.interviews_audit_timestamp() CASCADE;

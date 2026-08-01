-- Down: drop recruitment.candidates table
DROP TABLE IF EXISTS recruitment.candidates CASCADE;
DROP FUNCTION IF EXISTS recruitment.candidates_audit_timestamp() CASCADE;

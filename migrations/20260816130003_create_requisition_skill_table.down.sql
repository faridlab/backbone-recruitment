-- Down: drop recruitment.requisition_skills table
DROP TABLE IF EXISTS recruitment.requisition_skills CASCADE;
DROP FUNCTION IF EXISTS recruitment.requisition_skills_audit_timestamp() CASCADE;

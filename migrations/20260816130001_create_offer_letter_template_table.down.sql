-- Down: drop recruitment.offer_letter_templates table
DROP TABLE IF EXISTS recruitment.offer_letter_templates CASCADE;
DROP FUNCTION IF EXISTS recruitment.offer_letter_templates_audit_timestamp() CASCADE;

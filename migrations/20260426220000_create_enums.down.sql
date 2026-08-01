-- Down: drop enum types for recruitment module
DROP TYPE IF EXISTS requisition_status CASCADE;
DROP TYPE IF EXISTS offer_status CASCADE;
DROP TYPE IF EXISTS application_status CASCADE;
DROP TYPE IF EXISTS interview_status CASCADE;
DROP TYPE IF EXISTS candidate_source CASCADE;

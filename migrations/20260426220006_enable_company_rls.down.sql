-- Down: remove the company RLS fence for recruitment module

-- Reverse the company RLS fence for recruitment.candidates
DROP POLICY IF EXISTS candidates_company_isolation ON recruitment.candidates;
ALTER TABLE recruitment.candidates NO FORCE ROW LEVEL SECURITY;
ALTER TABLE recruitment.candidates DISABLE ROW LEVEL SECURITY;

-- Reverse the company RLS fence for recruitment.interviews
DROP POLICY IF EXISTS interviews_company_isolation ON recruitment.interviews;
ALTER TABLE recruitment.interviews NO FORCE ROW LEVEL SECURITY;
ALTER TABLE recruitment.interviews DISABLE ROW LEVEL SECURITY;

-- Reverse the company RLS fence for recruitment.job_applications
DROP POLICY IF EXISTS job_applications_company_isolation ON recruitment.job_applications;
ALTER TABLE recruitment.job_applications NO FORCE ROW LEVEL SECURITY;
ALTER TABLE recruitment.job_applications DISABLE ROW LEVEL SECURITY;

-- Reverse the company RLS fence for recruitment.job_offers
DROP POLICY IF EXISTS job_offers_company_isolation ON recruitment.job_offers;
ALTER TABLE recruitment.job_offers NO FORCE ROW LEVEL SECURITY;
ALTER TABLE recruitment.job_offers DISABLE ROW LEVEL SECURITY;

-- Reverse the company RLS fence for recruitment.job_requisitions
DROP POLICY IF EXISTS job_requisitions_company_isolation ON recruitment.job_requisitions;
ALTER TABLE recruitment.job_requisitions NO FORCE ROW LEVEL SECURITY;
ALTER TABLE recruitment.job_requisitions DISABLE ROW LEVEL SECURITY;


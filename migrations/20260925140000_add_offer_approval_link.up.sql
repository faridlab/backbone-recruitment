-- Migration: job_offers gain the approvals-engine link
--
-- The engine-gated lane (#550): create_draft files into the engine (the
-- `offer` resource type) and stamps the link; extend fails closed unless
-- the engine says Approved. NULL keeps the direct verb for unwired
-- deployments and rows created before the link existed.

ALTER TABLE recruitment.job_offers
    ADD COLUMN IF NOT EXISTS approval_request_id uuid;

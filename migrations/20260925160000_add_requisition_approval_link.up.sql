-- Migration: job_requisitions gain the approvals-engine link
--
-- The engine-gated open (#550): file_open stages the draft into the engine
-- (the requisition resource type) and stamps the link; confirm_open fails
-- closed unless the engine says Approved. NULL keeps the direct lane for
-- unwired deployments and rows created before the link existed.

ALTER TABLE recruitment.job_requisitions
    ADD COLUMN IF NOT EXISTS approval_request_id uuid;

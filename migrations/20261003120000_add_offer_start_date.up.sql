-- Migration: job_offers record the promised first working day
--
-- The extend verb takes the start date the offer letter promises; until now
-- it was only rendered into the letter and then lost, so the hire handed the
-- employee side the day of the hire as the join date instead of the agreed
-- one. The column keeps that date on the offer: extend writes it, the hire
-- event carries it. NULL = no date agreed (the letter shows the template's
-- fallback wording and the hire falls back to the day of acceptance).

ALTER TABLE recruitment.job_offers
    ADD COLUMN IF NOT EXISTS start_date date;

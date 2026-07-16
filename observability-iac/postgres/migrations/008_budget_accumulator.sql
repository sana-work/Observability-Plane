-- budget_accumulator — the agreed Redis stand-in (Redis onboarding pending).
-- Enrichment stage 6 calls observability.add_spend() per costed event; the
-- function is a single atomic upsert and returns whether the alert threshold
-- or the cap was crossed BY THIS CALL, so exactly one
-- BUDGET_THRESHOLD_EXCEEDED event is emitted per crossing.
-- Swap path: replace add_spend() calls with Redis INCRBYFLOAT + a threshold
-- check; the table then becomes a nightly reconciliation target.

CREATE TABLE IF NOT EXISTS observability.budget_accumulator (
  application_id  VARCHAR(64) NOT NULL,
  model_name      TEXT        NOT NULL DEFAULT '*',
  period          VARCHAR(16) NOT NULL,           -- daily | weekly | monthly
  period_start    DATE        NOT NULL,           -- day / ISO-week Monday / first-of-month
  spend_usd       NUMERIC(14,6) NOT NULL DEFAULT 0,
  alert_emitted   BOOLEAN     NOT NULL DEFAULT false,
  cap_emitted     BOOLEAN     NOT NULL DEFAULT false,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (application_id, model_name, period, period_start)
);

CREATE OR REPLACE FUNCTION observability.add_spend(
  p_application_id VARCHAR(64),
  p_model_name     TEXT,
  p_cost_usd       NUMERIC
) RETURNS TABLE (
  period          VARCHAR(16),
  new_spend_usd   NUMERIC,
  max_spend_usd   NUMERIC,
  alert_crossed   BOOLEAN,   -- true only on the call that crosses alert_at_pct
  cap_crossed     BOOLEAN    -- true only on the call that crosses 100%
) LANGUAGE plpgsql AS $$
DECLARE
  lim RECORD;
  v_period_start DATE;
  v_new NUMERIC;
  v_alert BOOLEAN;
  v_cap BOOLEAN;
BEGIN
  FOR lim IN
    SELECT bl.period AS lim_period, bl.max_spend_usd, bl.alert_at_pct, bl.model_name AS lim_model
    FROM observability.budget_limits bl
    WHERE bl.application_id = p_application_id
      AND bl.model_name IN (p_model_name, '*')
  LOOP
    v_period_start := CASE lim.lim_period
      WHEN 'daily'   THEN CURRENT_DATE
      WHEN 'weekly'  THEN date_trunc('week', CURRENT_DATE)::date
      ELSE                date_trunc('month', CURRENT_DATE)::date
    END;

    INSERT INTO observability.budget_accumulator AS acc
      (application_id, model_name, period, period_start, spend_usd)
    VALUES (p_application_id, lim.lim_model, lim.lim_period, v_period_start, p_cost_usd)
    ON CONFLICT (application_id, model_name, period, period_start)
    DO UPDATE SET spend_usd = acc.spend_usd + EXCLUDED.spend_usd,
                  updated_at = now()
    RETURNING acc.spend_usd INTO v_new;

    -- flip alert/cap flags atomically so only one caller sees the crossing
    UPDATE observability.budget_accumulator acc
    SET alert_emitted = true
    WHERE acc.application_id = p_application_id AND acc.model_name = lim.lim_model
      AND acc.period = lim.lim_period AND acc.period_start = v_period_start
      AND NOT acc.alert_emitted
      AND v_new >= lim.max_spend_usd * lim.alert_at_pct / 100.0
    RETURNING true INTO v_alert;

    UPDATE observability.budget_accumulator acc
    SET cap_emitted = true
    WHERE acc.application_id = p_application_id AND acc.model_name = lim.lim_model
      AND acc.period = lim.lim_period AND acc.period_start = v_period_start
      AND NOT acc.cap_emitted
      AND v_new >= lim.max_spend_usd
    RETURNING true INTO v_cap;

    period := lim.lim_period;
    new_spend_usd := v_new;
    max_spend_usd := lim.max_spend_usd;
    alert_crossed := COALESCE(v_alert, false);
    cap_crossed := COALESCE(v_cap, false);
    v_alert := NULL; v_cap := NULL;
    RETURN NEXT;
  END LOOP;
END;
$$;

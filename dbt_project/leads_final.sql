-- ============================================================
-- MODEL: leads_final  (MARTS LAYER)
-- ============================================================
-- PURPOSE:
--     The final, clean, analysis-ready table.
--     This is the single source of truth consumed by:
--       → Layer 3: Scoring model (scikit-learn reads this)
--       → Layer 4: AI Agents (LangChain/CrewAI reads this)
--       → Layer 5: GTM Automation (routing decisions)
--       → FastAPI:  /leads endpoint serves this
--
-- RULES FOR MART MODELS:
--     ✅ Select only the columns downstream consumers need
--     ✅ Apply final filters (remove junk/invalid leads)
--     ✅ This is the "product" dbt delivers to the business
--     ❌ No heavy transformations (that happened in intermediate)
--
-- THINK OF THIS AS:
--     The "clean room" at the end of the assembly line.
--     Everything that enters here has been validated,
--     cleaned, and enriched. Nothing dirty gets through.
--
-- MATERIALIZED AS: table
--     Marts are always tables — they're queried frequently
--     by multiple consumers and need to be fast.
-- ============================================================

WITH enriched AS (
    SELECT * FROM {{ ref('leads_enriched') }}
)

SELECT
    -- ── CORE IDENTIFIERS ─────────────────────────────────
    lead_id,
    lead_name,
    email,
    company_name,
    job_title,

    -- ── FIRMOGRAPHICS ────────────────────────────────────
    industry,
    employee_count,
    annual_revenue_usd,
    country,
    company_tier,       -- startup / smb / mid_market / enterprise
    revenue_band,       -- micro / small / mid / large

    -- ── LEAD SIGNALS ─────────────────────────────────────
    lead_source,
    days_since_last_activity,
    website_visits,
    emails_opened,
    demo_requests,
    engagement_score,   -- 0-100 composite score from intermediate

    -- ── DERIVED INTELLIGENCE ─────────────────────────────
    lead_temperature,   -- hot / warm / cool / cold
    has_budget,
    in_buying_stage,
    is_icp_fit,         -- TRUE/FALSE — key routing signal

    -- ── AUDIT ────────────────────────────────────────────
    lead_created_date,
    dbt_updated_at

FROM enriched

-- ── FINAL FILTER ─────────────────────────────────────────
-- Remove leads that are clearly dead — no activity in 6 months
-- AND zero engagement signals. Not worth scoring or routing.
WHERE NOT (
    days_since_last_activity > 180
    AND engagement_score     = 0
    AND demo_requests        = 0
)

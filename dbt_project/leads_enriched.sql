-- ============================================================
-- MODEL: leads_enriched  (INTERMEDIATE LAYER)
-- ============================================================
-- PURPOSE:
--     Add business logic and derived fields on top of stg_leads.
--     This is where "raw data" becomes "intelligent context."
--
-- WHAT WE ADD HERE:
--     - lead_temperature  → hot / warm / cool / cold
--     - engagement_score  → 0-100 composite behavioral score
--     - company_tier      → startup / smb / mid_market / enterprise
--     - revenue_band      → micro / small / mid / large
--     - is_icp_fit        → does this lead match our ideal customer?
--
-- WHY THIS MATTERS:
--     The scoring model (Layer 3) and AI agents (Layer 4) consume
--     this table. These derived fields give them richer signals
--     than the raw numbers alone.
--
--     Example: agent doesn't just see "last_activity_days = 3"
--     It sees "lead_temperature = hot" — pre-reasoned for it.
--
-- MATERIALIZED AS: table
--     Intermediate models are stored as actual tables because
--     they're more expensive to compute and used by multiple
--     downstream models.
-- ============================================================

WITH base AS (
    -- Reference stg_leads using dbt's ref() function
    -- ref() tells dbt this model depends on stg_leads
    -- dbt will always run stg_leads FIRST, then this model
    SELECT * FROM {{ ref('stg_leads') }}
)

SELECT
    -- Pass through everything from staging
    *,

    -- ── LEAD TEMPERATURE ─────────────────────────────────
    -- How recently did this lead engage?
    -- This is the single most important recency signal.
    CASE
        WHEN days_since_last_activity <= 7  THEN 'hot'   -- active this week
        WHEN days_since_last_activity <= 30 THEN 'warm'  -- active this month
        WHEN days_since_last_activity <= 90 THEN 'cool'  -- active this quarter
        ELSE                                     'cold'  -- gone quiet
    END                                             AS lead_temperature,

    -- ── ENGAGEMENT SCORE (0–100) ──────────────────────────
    -- Composite score combining all behavioral signals.
    -- Weights reflect real GTM logic:
    --   demo requests are the strongest buying signal (worth 20pts each)
    --   emails opened show intent (5pts each)
    --   website visits show awareness (3pts each)
    --   recent activity gets a bonus
    LEAST(100,
        (demo_requests  * 20) +
        (emails_opened  * 5)  +
        (website_visits * 3)  +
        CASE
            WHEN days_since_last_activity <= 7  THEN 20
            WHEN days_since_last_activity <= 14 THEN 10
            ELSE 0
        END
    )                                               AS engagement_score,

    -- ── COMPANY TIER ──────────────────────────────────────
    -- Segment companies by size — routing rules differ per tier.
    -- Enterprise leads go to senior AEs, startups to SDRs, etc.
    CASE
        WHEN employee_count >= 1000 THEN 'enterprise'
        WHEN employee_count >= 200  THEN 'mid_market'
        WHEN employee_count >= 50   THEN 'smb'
        ELSE                             'startup'
    END                                             AS company_tier,

    -- ── REVENUE BAND ──────────────────────────────────────
    CASE
        WHEN annual_revenue_usd >= 100000000 THEN 'large'
        WHEN annual_revenue_usd >= 10000000  THEN 'mid'
        WHEN annual_revenue_usd >= 1000000   THEN 'small'
        ELSE                                      'micro'
    END                                             AS revenue_band,

    -- ── ICP FIT FLAG ──────────────────────────────────────
    -- ICP = Ideal Customer Profile
    -- A lead is ICP fit if they have: budget + buying intent + recent activity
    -- This is a hard yes/no signal the routing agent uses directly
    CASE
        WHEN has_budget       = TRUE
         AND in_buying_stage  = TRUE
         AND days_since_last_activity <= 30
        THEN TRUE
        ELSE FALSE
    END                                             AS is_icp_fit

FROM base

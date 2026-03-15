-- ============================================================
-- MODEL: stg_leads  (STAGING LAYER)
-- ============================================================
-- PURPOSE:
--     Clean and standardize raw CRM data coming from raw_leads.
--     This is the first transformation step.
--
-- RULES FOR STAGING MODELS:
--     ✅ Rename columns to consistent snake_case
--     ✅ Cast data types correctly (string → integer, etc.)
--     ✅ Normalize messy values (e.g. 'true'/'True' → boolean)
--     ✅ Light cleaning only
--     ❌ NO business logic here (no scoring, no categorizing)
--     ❌ NO joining other tables
--
-- REAL WORLD EQUIVALENT:
--     In production, staging models sit right after raw ingestion.
--     Every source system (Salesforce, HubSpot, Marketo) gets its
--     own staging model. This keeps source-specific messiness
--     isolated and never bleeds into downstream models.
--
-- MATERIALIZED AS: view
--     Staging models are views — they don't store data, they just
--     define a query. Lightweight and always up to date.
-- ============================================================

SELECT
    -- ── IDENTIFIERS ──────────────────────────────────────
    id                                              AS lead_id,
    name                                            AS lead_name,
    lower(trim(email))                              AS email,        -- normalize email to lowercase
    company                                         AS company_name,

    -- ── FIRMOGRAPHICS ────────────────────────────────────
    industry,
    CAST(employee_count  AS INTEGER)                AS employee_count,
    CAST(annual_revenue  AS BIGINT)                 AS annual_revenue_usd,
    country,
    job_title,

    -- ── LEAD METADATA ────────────────────────────────────
    lead_source,
    created_date                                    AS lead_created_date,

    -- ── ACTIVITY SIGNALS (cast to proper numbers) ────────
    CAST(last_activity_days   AS INTEGER)           AS days_since_last_activity,
    CAST(num_website_visits   AS INTEGER)           AS website_visits,
    CAST(num_emails_opened    AS INTEGER)           AS emails_opened,
    CAST(num_demo_requests    AS INTEGER)           AS demo_requests,

    -- ── BUYING SIGNALS (normalize string → boolean) ──────
    -- Raw CSV has 'true'/'false' as strings — convert to real booleans
    CASE
        WHEN lower(trim(has_budget))      = 'true' THEN TRUE ELSE FALSE
    END                                             AS has_budget,

    CASE
        WHEN lower(trim(in_buying_stage)) = 'true' THEN TRUE ELSE FALSE
    END                                             AS in_buying_stage,

    -- ── AUDIT COLUMN ─────────────────────────────────────
    -- Track when dbt processed this row
    CURRENT_TIMESTAMP                               AS dbt_updated_at

FROM raw_leads

/* ============================================================
    Profit & Loss Statement | PostgreSQL
   ============================================================
   Staging tables used:
     "sap_stg_ecc"."stg_sap_pl_glt0_rpt"  — G/L Totals (Reporting Year)
     "sap_stg_ecc"."stg_sap_pl_glt0_cmp"  — G/L Totals (Comparison Year)
     "sap_stg_ecc"."stg_sap_pl_skat"      — G/L Account Texts
     "sap_stg_ecc"."stg_sap_pl_ska1"      — G/L Account Master
     "sap_stg_ecc"."stg_sap_pl_t001"      — Company Code Master

   Key conversions from SQL Server:
     DECLARE @var          → CTE_PARAMS with literal values
     ISNULL(x, y)          → COALESCE(x, y)
     TRY_CAST(x AS ...)    → CAST(NULLIF(x,'') AS ...)
     [dbo].[TABLE]         → "sap_stg_ecc"."stg_sap_pl_<table>"
     [Column Alias]        → "Column Alias"
     ISNULL(cmp.RYEAR,'') → COALESCE(cmp.ryear, '')

   Period parameters: edit date_from / date_to in CTE_PARAMS.
   For full-year:  period_from = 1, period_to = 12.
   For Q1 only:    period_from = 1, period_to = 3.
   ============================================================ */

WITH CTE_PARAMS AS (
    SELECT
        1  AS period_from,   -- First period to include
        12 AS period_to,     -- Last  period to include
        'E' AS language_key  -- Language for account texts
),

/* ──────────────────────────────────────────────────────────────
   STEP 1 — CTE: Period summing for the Reporting Year (GLT0_RPT)
   Sums TSL01–TSL16 (transaction currency) and HSL01–HSL16
   (local currency) only for the selected period range.
   CAST(NULLIF(col,'') AS NUMERIC) replaces TRY_CAST + ISNULL.
   ────────────────────────────────────────────────────────────── */
CTE_GLT0_PERIOD_SUM AS (
    SELECT
        r.bukrs,
        r.ryear,
        r.racct,
        r.rbusa,
        r.rtcur,
        r.rldnr,
        r.rrcty,
        r.rvers,
        r.drcrk,

        /* ── Transaction currency period total ──────────────── */
        (
            CASE WHEN p.period_from <=  1 AND p.period_to >=  1 THEN COALESCE(CAST(NULLIF(r.tsl01,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  2 AND p.period_to >=  2 THEN COALESCE(CAST(NULLIF(r.tsl02,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  3 AND p.period_to >=  3 THEN COALESCE(CAST(NULLIF(r.tsl03,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  4 AND p.period_to >=  4 THEN COALESCE(CAST(NULLIF(r.tsl04,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  5 AND p.period_to >=  5 THEN COALESCE(CAST(NULLIF(r.tsl05,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  6 AND p.period_to >=  6 THEN COALESCE(CAST(NULLIF(r.tsl06,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  7 AND p.period_to >=  7 THEN COALESCE(CAST(NULLIF(r.tsl07,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  8 AND p.period_to >=  8 THEN COALESCE(CAST(NULLIF(r.tsl08,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  9 AND p.period_to >=  9 THEN COALESCE(CAST(NULLIF(r.tsl09,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 10 AND p.period_to >= 10 THEN COALESCE(CAST(NULLIF(r.tsl10,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 11 AND p.period_to >= 11 THEN COALESCE(CAST(NULLIF(r.tsl11,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 12 AND p.period_to >= 12 THEN COALESCE(CAST(NULLIF(r.tsl12,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 13 AND p.period_to >= 13 THEN COALESCE(CAST(NULLIF(r.tsl13,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 14 AND p.period_to >= 14 THEN COALESCE(CAST(NULLIF(r.tsl14,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 15 AND p.period_to >= 15 THEN COALESCE(CAST(NULLIF(r.tsl15,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 16 AND p.period_to >= 16 THEN COALESCE(CAST(NULLIF(r.tsl16,'') AS NUMERIC(18,2)), 0) ELSE 0 END
        )                                                   AS period_total_tsl,

        /* ── Local (company code) currency period total ─────── */
        (
            CASE WHEN p.period_from <=  1 AND p.period_to >=  1 THEN COALESCE(CAST(NULLIF(r.hsl01,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  2 AND p.period_to >=  2 THEN COALESCE(CAST(NULLIF(r.hsl02,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  3 AND p.period_to >=  3 THEN COALESCE(CAST(NULLIF(r.hsl03,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  4 AND p.period_to >=  4 THEN COALESCE(CAST(NULLIF(r.hsl04,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  5 AND p.period_to >=  5 THEN COALESCE(CAST(NULLIF(r.hsl05,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  6 AND p.period_to >=  6 THEN COALESCE(CAST(NULLIF(r.hsl06,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  7 AND p.period_to >=  7 THEN COALESCE(CAST(NULLIF(r.hsl07,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  8 AND p.period_to >=  8 THEN COALESCE(CAST(NULLIF(r.hsl08,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  9 AND p.period_to >=  9 THEN COALESCE(CAST(NULLIF(r.hsl09,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 10 AND p.period_to >= 10 THEN COALESCE(CAST(NULLIF(r.hsl10,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 11 AND p.period_to >= 11 THEN COALESCE(CAST(NULLIF(r.hsl11,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 12 AND p.period_to >= 12 THEN COALESCE(CAST(NULLIF(r.hsl12,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 13 AND p.period_to >= 13 THEN COALESCE(CAST(NULLIF(r.hsl13,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 14 AND p.period_to >= 14 THEN COALESCE(CAST(NULLIF(r.hsl14,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 15 AND p.period_to >= 15 THEN COALESCE(CAST(NULLIF(r.hsl15,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 16 AND p.period_to >= 16 THEN COALESCE(CAST(NULLIF(r.hsl16,'') AS NUMERIC(18,2)), 0) ELSE 0 END
        )                                                   AS period_total_hsl,

        /* Opening balance carried forward */
        COALESCE(CAST(NULLIF(r.tslvt,'') AS NUMERIC(18,2)), 0) AS opening_bal_tsl,
        COALESCE(CAST(NULLIF(r.hslvt,'') AS NUMERIC(18,2)), 0) AS opening_bal_hsl

    FROM "sap_stg_ecc"."stg_sap_pl_glt0_rpt" r
    CROSS JOIN CTE_PARAMS p
),

/* ──────────────────────────────────────────────────────────────
   STEP 2 — CTE: Same period summing for the Comparison Year
   ────────────────────────────────────────────────────────────── */
CTE_GLT0_CMP_SUM AS (
    SELECT
        c.bukrs,
        c.ryear,
        c.racct,
        c.rbusa,
        c.rtcur,
        c.rldnr,
        c.rrcty,
        c.rvers,
        c.drcrk,
        (
            CASE WHEN p.period_from <=  1 AND p.period_to >=  1 THEN COALESCE(CAST(NULLIF(c.tsl01,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  2 AND p.period_to >=  2 THEN COALESCE(CAST(NULLIF(c.tsl02,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  3 AND p.period_to >=  3 THEN COALESCE(CAST(NULLIF(c.tsl03,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  4 AND p.period_to >=  4 THEN COALESCE(CAST(NULLIF(c.tsl04,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  5 AND p.period_to >=  5 THEN COALESCE(CAST(NULLIF(c.tsl05,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  6 AND p.period_to >=  6 THEN COALESCE(CAST(NULLIF(c.tsl06,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  7 AND p.period_to >=  7 THEN COALESCE(CAST(NULLIF(c.tsl07,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  8 AND p.period_to >=  8 THEN COALESCE(CAST(NULLIF(c.tsl08,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  9 AND p.period_to >=  9 THEN COALESCE(CAST(NULLIF(c.tsl09,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 10 AND p.period_to >= 10 THEN COALESCE(CAST(NULLIF(c.tsl10,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 11 AND p.period_to >= 11 THEN COALESCE(CAST(NULLIF(c.tsl11,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 12 AND p.period_to >= 12 THEN COALESCE(CAST(NULLIF(c.tsl12,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 13 AND p.period_to >= 13 THEN COALESCE(CAST(NULLIF(c.tsl13,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 14 AND p.period_to >= 14 THEN COALESCE(CAST(NULLIF(c.tsl14,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 15 AND p.period_to >= 15 THEN COALESCE(CAST(NULLIF(c.tsl15,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 16 AND p.period_to >= 16 THEN COALESCE(CAST(NULLIF(c.tsl16,'') AS NUMERIC(18,2)), 0) ELSE 0 END
        )                                                   AS period_total_tsl,
        (
            CASE WHEN p.period_from <=  1 AND p.period_to >=  1 THEN COALESCE(CAST(NULLIF(c.hsl01,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  2 AND p.period_to >=  2 THEN COALESCE(CAST(NULLIF(c.hsl02,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  3 AND p.period_to >=  3 THEN COALESCE(CAST(NULLIF(c.hsl03,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  4 AND p.period_to >=  4 THEN COALESCE(CAST(NULLIF(c.hsl04,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  5 AND p.period_to >=  5 THEN COALESCE(CAST(NULLIF(c.hsl05,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  6 AND p.period_to >=  6 THEN COALESCE(CAST(NULLIF(c.hsl06,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  7 AND p.period_to >=  7 THEN COALESCE(CAST(NULLIF(c.hsl07,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  8 AND p.period_to >=  8 THEN COALESCE(CAST(NULLIF(c.hsl08,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <=  9 AND p.period_to >=  9 THEN COALESCE(CAST(NULLIF(c.hsl09,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 10 AND p.period_to >= 10 THEN COALESCE(CAST(NULLIF(c.hsl10,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 11 AND p.period_to >= 11 THEN COALESCE(CAST(NULLIF(c.hsl11,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 12 AND p.period_to >= 12 THEN COALESCE(CAST(NULLIF(c.hsl12,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 13 AND p.period_to >= 13 THEN COALESCE(CAST(NULLIF(c.hsl13,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 14 AND p.period_to >= 14 THEN COALESCE(CAST(NULLIF(c.hsl14,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 15 AND p.period_to >= 15 THEN COALESCE(CAST(NULLIF(c.hsl15,'') AS NUMERIC(18,2)), 0) ELSE 0 END
          + CASE WHEN p.period_from <= 16 AND p.period_to >= 16 THEN COALESCE(CAST(NULLIF(c.hsl16,'') AS NUMERIC(18,2)), 0) ELSE 0 END
        )                                                   AS period_total_hsl,
        COALESCE(CAST(NULLIF(c.tslvt,'') AS NUMERIC(18,2)), 0) AS opening_bal_tsl,
        COALESCE(CAST(NULLIF(c.hslvt,'') AS NUMERIC(18,2)), 0) AS opening_bal_hsl

    FROM "sap_stg_ecc"."stg_sap_pl_glt0_cmp" c
    CROSS JOIN CTE_PARAMS p
),

/* ──────────────────────────────────────────────────────────────
   STEP 3 — CTE: Account text from SKAT
   ────────────────────────────────────────────────────────────── */
CTE_ACCT_TEXT AS (
    SELECT
        ktopl,
        saknr,
        txt20   AS acct_short_text,
        txt50   AS acct_long_text
    FROM "sap_stg_ecc"."stg_sap_pl_skat"
    -- Filter by language key — join to CTE_PARAMS for dynamic lang
),

/* ──────────────────────────────────────────────────────────────
   STEP 4 — CTE: Account master from SKA1
   XBILK = ''  → P&L account
   XBILK = 'X' → Balance sheet account
   ────────────────────────────────────────────────────────────── */
CTE_ACCT_MASTER AS (
    SELECT
        ktopl,
        saknr,
        xbilk,
        CASE xbilk
            WHEN 'X' THEN 'Balance Sheet'
            WHEN ''  THEN 'P&L'
            ELSE          'Unknown'
        END         AS acct_type,
        gvtyp       AS pl_acct_type,
        ktoks       AS acct_group,
        bilkt       AS group_acct_nbr
    FROM "sap_stg_ecc"."stg_sap_pl_ska1"
),

/* ──────────────────────────────────────────────────────────────
   STEP 5 — CTE: Company code currency from T001
   ────────────────────────────────────────────────────────────── */
CTE_COMPANY AS (
    SELECT
        bukrs,
        butxt   AS company_name,
        waers   AS local_currency,
        ktopl,
        periv   AS fiscal_year_variant
    FROM "sap_stg_ecc"."stg_sap_pl_t001"
)

/* ──────────────────────────────────────────────────────────────
   MAIN SELECT — P&L Report Columns
   ────────────────────────────────────────────────────────────── */
SELECT

    /* ── Identity ───────────────────────────────────────────── */
    rpt.bukrs                                       AS "Company Code",
    co.company_name                                 AS "Company Name",
    rpt.racct                                       AS "Account Number",

    /* ── Account Description ────────────────────────────────── */
    COALESCE(tx.acct_long_text,
        COALESCE(tx.acct_short_text, rpt.racct))    AS "Text for B/S P&L item",

    /* ── Account Classification ─────────────────────────────── */
    am.acct_type                                    AS "Account Type",
    am.pl_acct_type                                 AS "P&L Account Type",
    am.acct_group                                   AS "Account Group",
    am.group_acct_nbr                               AS "Group Account",

    /* ── Organisation ───────────────────────────────────────── */
    rpt.rbusa                                       AS "Business Area",
    rpt.rldnr                                       AS "Ledger",
    rpt.rvers                                       AS "Version",
    rpt.rtcur                                       AS "Transaction Currency",
    co.local_currency                               AS "Local Currency",
    rpt.ryear                                       AS "Reporting Year",
    COALESCE(cmp.ryear, '')                         AS "Comparison Year",
    rpt.drcrk                                       AS "Dr/Cr Indicator",

    /* ── Opening Balance (carry-forward) ────────────────────── */
    rpt.opening_bal_tsl                             AS "Opening Bal. (Trans. Curr.)",
    rpt.opening_bal_hsl                             AS "Opening Bal. (Local Curr.)",

    /* ── Total of Reporting Period (transaction currency) ───── */
    rpt.period_total_tsl                            AS "Total of reporting period",

    /* ── Total of Comparison Period (transaction currency) ──── */
    COALESCE(cmp.period_total_tsl, 0)              AS "Total of comparison period",

    /* ── Absolute Difference ────────────────────────────────── */
    rpt.period_total_tsl
        - COALESCE(cmp.period_total_tsl, 0)        AS "Absolute difference",

    /* ── Local Currency Columns ─────────────────────────────── */
    rpt.period_total_hsl                            AS "Total of reporting period (LC)",
    COALESCE(cmp.period_total_hsl, 0)              AS "Total of comparison period (LC)",
    rpt.period_total_hsl
        - COALESCE(cmp.period_total_hsl, 0)        AS "Absolute difference (LC)"

FROM       CTE_GLT0_PERIOD_SUM          rpt

/* ── Comparison year figures ────────────────────────────────── */
LEFT JOIN  CTE_GLT0_CMP_SUM             cmp
    ON  cmp.bukrs  = rpt.bukrs
    AND cmp.racct  = rpt.racct
    AND cmp.rbusa  = rpt.rbusa
    AND cmp.rtcur  = rpt.rtcur
    AND cmp.rldnr  = rpt.rldnr
    AND cmp.rrcty  = rpt.rrcty
    AND cmp.rvers  = rpt.rvers

/* ── Company code master (name + currency) ──────────────────── */
LEFT JOIN  CTE_COMPANY                  co
    ON  co.bukrs  = rpt.bukrs

/* ── Account long text (filter by language key) ─────────────── */
LEFT JOIN  CTE_ACCT_TEXT                tx
    ON  tx.saknr  = rpt.racct
    AND tx.ktopl  = co.ktopl

/* ── Account master (B/S vs P&L flag, account group) ────────── */
LEFT JOIN  CTE_ACCT_MASTER              am
    ON  am.saknr  = rpt.racct
    AND am.ktopl  = co.ktopl

ORDER BY
    rpt.bukrs   ASC,
    rpt.racct   ASC,
    rpt.rbusa   ASC,
    rpt.rtcur   ASC;

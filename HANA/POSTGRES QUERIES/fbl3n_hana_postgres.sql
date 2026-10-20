/* ============================================================
    FBL3N — G/L Account Line Items Report (UGX/RWF Updated) | PostgreSQL
   ============================================================
   Source tables (staged via Python extractor):
     • stg_sap_bsis  — Open G/L line items   (BSIS secondary index)
     • stg_sap_bsas  — Cleared G/L line items (BSAS secondary index)
     • stg_sap_t001  — Company Code master data

   Design logic:
     • Multiplies amounts by 100 for UGX and RWF to correct
       SAP's internal decimal storage for zero-decimal currencies.
     • Maintains 1:1 ratio for USD and other currencies.
     • Debit/Credit sign derived from SHKZG ('S' = Debit, 'H' = Credit).
     • Year/Month derived from GJAHR + MONAT (fiscal period).
     • Net Due Date derived from ZFBDT (baseline date) — no ZBD3T in
       BSIS/BSAS; G/L items typically rely on baseline date directly.
   ============================================================ */

WITH CTE_LINE_ITEMS AS (
    SELECT
        'Open'              AS item_status,      -- BSIS = open items
        bukrs, hkont, belnr, gjahr, buzei,
        monat, budat, bldat, blart, xblnr,
        zuonr, bschl, shkzg,
        wrbtr, waers, dmbtr,
        mwsts, wmwst,
        sgtxt, zfbdt,
        gsber, mwskz, kostl, prctr,
        umskz, vbund,
        augdt, augbl                             -- blank for open items
    FROM "sap_stg_hana"."stg_sap_bsis"

    UNION ALL

    SELECT
        'Cleared'           AS item_status,      -- BSAS = cleared items
        bukrs, hkont, belnr, gjahr, buzei,
        monat, budat, bldat, blart, xblnr,
        zuonr, bschl, shkzg,
        wrbtr, waers, dmbtr,
        mwsts, wmwst,
        sgtxt, zfbdt,
        gsber, mwskz, kostl, prctr,
        umskz, vbund,
        augdt, augbl                             -- populated for cleared items
    FROM "sap_stg_hana"."stg_sap_bsas"
),

CTE_COMPUTED AS (
    SELECT
        li.*,

        /* ── Date casts ──────────────────────────────────────── */
        CASE WHEN NULLIF(li.budat, '') IS NOT NULL
             THEN TO_DATE(li.budat, 'YYYYMMDD')
        END                                            AS posting_date,

        CASE WHEN NULLIF(li.bldat, '') IS NOT NULL
             THEN TO_DATE(li.bldat, 'YYYYMMDD')
        END                                            AS document_date,

        CASE WHEN NULLIF(li.zfbdt, '') IS NOT NULL
             THEN TO_DATE(li.zfbdt, 'YYYYMMDD')
        END                                            AS baseline_date,

        CASE WHEN NULLIF(li.augdt, '') IS NOT NULL
             THEN TO_DATE(li.augdt, 'YYYYMMDD')
        END                                            AS clearing_date,

        /* ── Year/Month from fiscal period (GJAHR + MONAT) ───── */
        /* MONAT is SAP fiscal period (01-12 or 01-16 for special) */
        NULLIF(li.gjahr, '') || '/'
            || LPAD(NULLIF(li.monat, ''), 2, '0')      AS year_month_fiscal,

        /* ── Signed amounts with Currency Correction Logic ──── */
        /* Multiplies by 100 if Currency is UGX or RWF          */
        (CASE
            WHEN li.waers IN ('UGX', 'RWF') THEN 100.0
            ELSE 1.0
         END) *
        (CASE li.shkzg
            WHEN 'H' THEN -1.0 * COALESCE(CAST(NULLIF(li.wrbtr, '') AS NUMERIC(18,2)), 0)
            ELSE            1.0 * COALESCE(CAST(NULLIF(li.wrbtr, '') AS NUMERIC(18,2)), 0)
        END)                                           AS amt_doc_curr_signed,

        (CASE
            WHEN li.waers IN ('UGX', 'RWF') THEN 100.0
            ELSE 1.0
         END) *
        (CASE li.shkzg
            WHEN 'H' THEN -1.0 * COALESCE(CAST(NULLIF(li.dmbtr, '') AS NUMERIC(18,2)), 0)
            ELSE            1.0 * COALESCE(CAST(NULLIF(li.dmbtr, '') AS NUMERIC(18,2)), 0)
        END)                                           AS amt_local_curr_signed,

        /* ── Tax amounts with Currency Correction Logic ─────── */
        (CASE
            WHEN li.waers IN ('UGX', 'RWF') THEN 100.0
            ELSE 1.0
         END) *
        COALESCE(CAST(NULLIF(li.mwsts, '') AS NUMERIC(18,2)), 0)
                                                       AS tax_amt_local_curr,

        (CASE
            WHEN li.waers IN ('UGX', 'RWF') THEN 100.0
            ELSE 1.0
         END) *
        COALESCE(CAST(NULLIF(li.wmwst, '') AS NUMERIC(18,2)), 0)
                                                       AS tax_amt_doc_curr,

        CASE li.shkzg
            WHEN 'S' THEN 'Debit'
            WHEN 'H' THEN 'Credit'
            ELSE li.shkzg
        END                                            AS dr_cr_text

    FROM CTE_LINE_ITEMS li
),

CTE_AGING AS (
    SELECT
        c.*,
        CASE
            WHEN c.item_status = 'Cleared'              THEN NULL
            WHEN c.baseline_date IS NULL                 THEN NULL
            WHEN c.baseline_date >= CURRENT_DATE         THEN NULL
            ELSE (CURRENT_DATE - c.baseline_date)
        END                                            AS days_overdue,

        CASE
            WHEN c.item_status = 'Cleared'              THEN 'Cleared'
            WHEN c.baseline_date IS NULL                 THEN 'No Due Date'
            WHEN c.baseline_date >= CURRENT_DATE         THEN 'Not Yet Due'
            WHEN (CURRENT_DATE - c.baseline_date) <= 30  THEN '01-30 Days'
            WHEN (CURRENT_DATE - c.baseline_date) <= 60  THEN '31-60 Days'
            WHEN (CURRENT_DATE - c.baseline_date) <= 90  THEN '61-90 Days'
            ELSE                                              '90+ Days'
        END                                            AS aging_bucket
    FROM CTE_COMPUTED c
)

/* ══════════════════════════════════════════════════════════════
    MAIN SELECT
   ══════════════════════════════════════════════════════════════ */
SELECT

    /* ── Status & document identity ─────────────────────────── */
    a.item_status                               AS "St",
    a.bukrs                                     AS "Company Code",
    COALESCE(NULLIF(t001.butxt, ''), '— not found —')
                                                AS "Company Name",
    t001.waers                                  AS "Local Currency",

    a.hkont                                     AS "G/L Account",
    a.belnr                                     AS "Document No.",
    a.gjahr                                     AS "Fiscal Year",
    a.buzei                                     AS "Line Item",
    a.monat                                     AS "Period",
    a.blart                                     AS "Doc. Type",

    CASE a.blart
        WHEN 'SA' THEN 'G/L Account Document'
        WHEN 'AB' THEN 'Accounting Document'
        WHEN 'DR' THEN 'Customer Invoice'
        WHEN 'DG' THEN 'Customer Credit Memo'
        WHEN 'DZ' THEN 'Customer Payment'
        WHEN 'KR' THEN 'Vendor Invoice'
        WHEN 'KG' THEN 'Vendor Credit Memo'
        WHEN 'KZ' THEN 'Vendor Payment'
        WHEN 'ZP' THEN 'Payment Posting'
        WHEN 'ZC' THEN 'Clearing Document'
        WHEN 'RE' THEN 'Invoice - Gross'
        WHEN 'RN' THEN 'Invoice - Net'
        WHEN 'WA' THEN 'Goods Issue'
        WHEN 'WE' THEN 'Goods Receipt'
        ELSE a.blart
    END                                         AS "Doc. Type Text",

    /* ── Reference & assignment ──────────────────────────────── */
    a.xblnr                                     AS "Reference",
    a.zuonr                                     AS "Assignment",
    a.sgtxt                                     AS "Text",

    /* ── Posting indicators ──────────────────────────────────── */
    a.bschl                                     AS "PK",
    a.shkzg                                     AS "Dr/Cr Indicator",
    a.dr_cr_text                                AS "Dr/Cr Text",
    a.umskz                                     AS "Special G/L Ind.",

    /* ── Dates ───────────────────────────────────────────────── */
    a.posting_date                              AS "Pstng Date",
    a.document_date                             AS "Doc. Date",
    a.baseline_date                             AS "Due Net",
    a.clearing_date                             AS "Clearing Date",
    a.augbl                                     AS "Clrng Doc.",

    /* ── Amounts ─────────────────────────────────────────────── */
    a.amt_doc_curr_signed                       AS "Amount in DC",
    a.waers                                     AS "Curr.",
    a.amt_local_curr_signed                     AS "Amount in LC",

    /* Absolute raw amounts with Currency Factor applied */
    CAST(NULLIF(a.wrbtr, '') AS NUMERIC(18,2)) *
    (CASE WHEN a.waers IN ('UGX', 'RWF') THEN 100.0 ELSE 1.0 END)
                                                AS "Abs. Amt in DC",

    CAST(NULLIF(a.dmbtr, '') AS NUMERIC(18,2)) *
    (CASE WHEN a.waers IN ('UGX', 'RWF') THEN 100.0 ELSE 1.0 END)
                                                AS "Abs. Amt in LC",

    /* ── Tax amounts ─────────────────────────────────────────── */
    a.tax_amt_local_curr                        AS "Tax Amt in LC",
    a.tax_amt_doc_curr                          AS "Tax Amt in DC",
    a.mwskz                                     AS "Tax Code",

    /* ── Aging (open items only) ─────────────────────────────── */
    a.days_overdue                              AS "Days Overdue",
    a.aging_bucket                              AS "Aging Bucket",

    /* ── Org fields ──────────────────────────────────────────── */
    a.gsber                                     AS "Business Area",
    a.kostl                                     AS "Cost Ctr",
    a.prctr                                     AS "Profit Ctr",
    a.vbund                                     AS "Trading Partner",

    /* ── Company Code master attributes ─────────────────────── */
    t001.ort01                                  AS "City",
    t001.land1                                  AS "Country",
    t001.ktopl                                  AS "Chart of Accounts",
    t001.periv                                  AS "Fiscal Year Variant",

    /* ── Year / period derived ───────────────────────────────── */
    NULLIF(a.gjahr, '')                         AS "Year",
    a.year_month_fiscal                         AS "Year/Month (Fiscal)",
    EXTRACT(YEAR FROM a.posting_date)           AS "Calendar Year",
    TO_CHAR(a.posting_date, 'YYYY/MM')          AS "Year/Month (Calendar)"

FROM CTE_AGING a
LEFT JOIN "sap_stg_hana"."stg_sap_t001" t001
    ON t001.bukrs = a.bukrs

ORDER BY
    a.hkont         ASC,
    a.bukrs         ASC,
    a.posting_date  DESC,
    a.belnr         ASC,
    a.buzei         ASC;
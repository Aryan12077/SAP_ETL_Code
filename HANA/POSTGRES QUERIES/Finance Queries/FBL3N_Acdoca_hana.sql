/* ============================================================
   FBL3N — G/L Account Line Items  |  SAP S/4HANA (ACDOCA) version
   ============================================================
   Source : sap_stg_hana.stg_sap_acdoca_acdoca  (KOART IN ('S','M'))
            sap_stg_hana.stg_sap_acdoca_t001
   Output : same 43 columns, names, order and data types as
            fbl3n_ecc_postgres.sql.

   ECC → ACDOCA differences handled here
     • Open / Cleared   : from AUGBL (blank = open), not BSIS/BSAS.
     • G/L lines        : KOART 'S' + 'M' (ECC BSIS excluded vendor /
                          customer reconciliation lines the same way).
     • Amounts          : HSL / WSL already signed; trailing minus parsed.
     • Currency factor  : UGX / RWF × 100 per amount's own currency
                          (HSL → RHCUR, WSL → RWCUR).
     • Period           : POPER (3 digits) shown as 2 digits like MONAT.
     • Due Net / aging  : ACDOCA.NETDT (ECC version used baseline ZFBDT).
     • Tax amounts      : ACDOCA has no MWSTS/WMWST; the amount of tax
                          lines (KTOSL MWS/VST/ESA/ESE) is shown, 0 on
                          other lines.
     • Reference (XBLNR): not in ACDOCA → NULL until BKPF is staged.
   ============================================================ */

WITH CTE_LINE_ITEMS AS (
    SELECT
        CASE WHEN NULLIF(a.augbl, '') IS NULL
             THEN 'Open' ELSE 'Cleared' END      AS item_status,
        a.rbukrs                                 AS bukrs,
        a.racct                                  AS hkont,
        a.belnr, a.gjahr, a.buzei,
        RIGHT(a.poper, 2)                        AS monat,
        a.budat, a.bldat, a.blart,
        a.zuonr, a.bschl,
        a.drcrk                                  AS shkzg,
        a.rwcur                                  AS waers,
        a.rhcur,
        a.ktosl,
        a.sgtxt, a.netdt,
        a.rbusa                                  AS gsber,
        a.mwskz,
        a.rcntr                                  AS kostl,
        a.prctr,
        a.augdt, a.augbl,

        /* Signed amounts: parse trailing minus, apply UGX/RWF factor */
        COALESCE(CASE WHEN TRIM(a.wsl) LIKE '%-'
                      THEN -1 * CAST(RTRIM(TRIM(a.wsl), '-') AS NUMERIC)
                      ELSE CAST(NULLIF(TRIM(a.wsl), '') AS NUMERIC) END, 0)
          * CASE WHEN a.rwcur IN ('UGX', 'RWF') THEN 100 ELSE 1 END
                                                 AS amt_doc_curr,
        COALESCE(CASE WHEN TRIM(a.hsl) LIKE '%-'
                      THEN -1 * CAST(RTRIM(TRIM(a.hsl), '-') AS NUMERIC)
                      ELSE CAST(NULLIF(TRIM(a.hsl), '') AS NUMERIC) END, 0)
          * CASE WHEN a.rhcur IN ('UGX', 'RWF') THEN 100 ELSE 1 END
                                                 AS amt_local_curr
    FROM "sap_stg_hana"."stg_sap_acdoca_acdoca" a
    WHERE a.rldnr = '0L'
      AND a.koart IN ('S', 'M')                  -- G/L and material account lines
),

CTE_COMPUTED AS (
    SELECT
        li.*,

        /* ── Date casts (blank or 00000000 = no date) ────────── */
        CASE WHEN NULLIF(NULLIF(li.budat, ''), '00000000') IS NOT NULL
             THEN TO_DATE(li.budat, 'YYYYMMDD')
        END                                            AS posting_date,

        CASE WHEN NULLIF(NULLIF(li.bldat, ''), '00000000') IS NOT NULL
             THEN TO_DATE(li.bldat, 'YYYYMMDD')
        END                                            AS document_date,

        CASE WHEN NULLIF(NULLIF(li.netdt, ''), '00000000') IS NOT NULL
             THEN TO_DATE(li.netdt, 'YYYYMMDD')
        END                                            AS baseline_date,   -- net due date (NETDT)

        CASE WHEN NULLIF(NULLIF(li.augdt, ''), '00000000') IS NOT NULL
             THEN TO_DATE(li.augdt, 'YYYYMMDD')
        END                                            AS clearing_date,

        /* ── Year/Month from fiscal period (GJAHR + POPER) ───── */
        NULLIF(li.gjahr, '') || '/'
            || LPAD(NULLIF(li.monat, ''), 2, '0')      AS year_month_fiscal,

        /* ── Signed amounts ──────────────────────────────────── */
        ROUND(li.amt_doc_curr, 2)       AS amt_doc_curr_signed,
        ROUND(li.amt_local_curr, 2)       AS amt_local_curr_signed,

        /* ── Tax amounts: tax lines only (unsigned, as ECC) ──── */
        ROUND(CASE WHEN li.ktosl IN ('MWS', 'VST', 'ESA', 'ESE')
                   THEN ABS(li.amt_local_curr) ELSE 0 END, 2)                         AS tax_amt_local_curr,

        ROUND(CASE WHEN li.ktosl IN ('MWS', 'VST', 'ESA', 'ESE')
                   THEN ABS(li.amt_doc_curr) ELSE 0 END, 2)                         AS tax_amt_doc_curr,

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
    MAIN SELECT (43 columns)
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
    NULL::TEXT                                  AS "Reference",   -- XBLNR: BKPF, not staged yet
    a.zuonr                                     AS "Assignment",
    a.sgtxt                                     AS "Text",

    /* ── Posting indicators ──────────────────────────────────── */
    a.bschl                                     AS "PK",
    a.shkzg                                     AS "Dr/Cr Indicator",
    a.dr_cr_text                                AS "Dr/Cr Text",

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

    /* Absolute amounts with currency factor applied */
    ABS(a.amt_doc_curr_signed)                  AS "Abs. Amt in DC",
    ABS(a.amt_local_curr_signed)                AS "Abs. Amt in LC",

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
LEFT JOIN "sap_stg_hana"."stg_sap_acdoca_t001" t001
    ON t001.bukrs = a.bukrs

ORDER BY
    a.hkont         ASC,
    a.bukrs         ASC,
    a.posting_date  DESC,
    a.belnr         ASC,
    a.buzei         ASC;
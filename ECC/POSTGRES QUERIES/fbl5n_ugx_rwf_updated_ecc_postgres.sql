/* ============================================================
    FBL5N — Customer Line Items Report (UGX/RWF Updated) | PostgreSQL
   ============================================================
   Design logic:
     • Multiplies amounts by 100 for UGX and RWF to correct
       SAP's internal decimal storage for zero-decimal currencies.
     • Maintains 1:1 ratio for USD and other currencies.
   ============================================================ */

WITH CTE_LINE_ITEMS AS (
    SELECT
        'Open'              AS item_status,      -- BSID = open items
        bukrs, kunnr, belnr, gjahr, buzei,
        budat, bldat, blart, xblnr, zuonr,
        hkont, umskz, bschl, shkzg,
        wrbtr, waers, dmbtr,
        sgtxt, rebzg, rebzj, zfbdt,
        gsber, mwskz, zterm, zbd3t,
        zlsch, zlspr, vbeln, kostl, prctr,
        augdt, augbl                             -- blank for open items
    FROM "sap_stg_ecc"."stg_sap_fbl5n_bsid"

    UNION ALL

    SELECT
        'Cleared'           AS item_status,      -- BSAD = cleared items
        bukrs, kunnr, belnr, gjahr, buzei,
        budat, bldat, blart, xblnr, zuonr,
        hkont, umskz, bschl, shkzg,
        wrbtr, waers, dmbtr,
        sgtxt, rebzg, rebzj, zfbdt,
        gsber, mwskz, zterm, zbd3t,
        zlsch, zlspr, vbeln, kostl, prctr,
        augdt, augbl                             -- populated for cleared items
    FROM "sap_stg_ecc"."stg_sap_fbl5n_bsad"
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

        /* ── Net Due Date = Baseline Date + Net Payment Days ─── */
        CASE
            WHEN NULLIF(li.zfbdt, '') IS NULL THEN NULL
            ELSE TO_DATE(li.zfbdt, 'YYYYMMDD')
                 + COALESCE(CAST(NULLIF(li.zbd3t, '') AS INTEGER), 0)
        END                                            AS net_due_date,

        /* ── Signed amounts with Currency Correction Logic ──── */
        /* Multiplies by 100 if Currency is UGX or RWF          */
        (CASE
            WHEN li.waers IN ('UGX', 'RWF') THEN 100.0
            ELSE 1.0
         END) *
        (CASE li.shkzg
            WHEN 'H' THEN -1.0 * COALESCE(CAST(NULLIF(li.wrbtr,'') AS NUMERIC(18,2)), 0)
            ELSE            1.0 * COALESCE(CAST(NULLIF(li.wrbtr,'') AS NUMERIC(18,2)), 0)
        END)                                           AS amt_doc_curr_signed,

        (CASE
            WHEN li.waers IN ('UGX', 'RWF') THEN 100.0
            ELSE 1.0
         END) *
        (CASE li.shkzg
            WHEN 'H' THEN -1.0 * COALESCE(CAST(NULLIF(li.dmbtr,'') AS NUMERIC(18,2)), 0)
            ELSE            1.0 * COALESCE(CAST(NULLIF(li.dmbtr,'') AS NUMERIC(18,2)), 0)
        END)                                           AS amt_local_curr_signed,

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
            WHEN c.item_status = 'Cleared'             THEN NULL
            WHEN c.net_due_date IS NULL                 THEN NULL
            WHEN c.net_due_date >= CURRENT_DATE         THEN NULL
            ELSE (CURRENT_DATE - c.net_due_date)
        END                                            AS days_overdue,

        CASE
            WHEN c.item_status = 'Cleared'             THEN 'Cleared'
            WHEN c.net_due_date IS NULL                 THEN 'No Due Date'
            WHEN c.net_due_date >= CURRENT_DATE         THEN 'Not Yet Due'
            WHEN (CURRENT_DATE - c.net_due_date) <= 30  THEN '01-30 Days'
            WHEN (CURRENT_DATE - c.net_due_date) <= 60  THEN '31-60 Days'
            WHEN (CURRENT_DATE - c.net_due_date) <= 90  THEN '61-90 Days'
            ELSE                                             '90+ Days'
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
    a.belnr                                     AS "Document No.",
    a.gjahr                                     AS "Fiscal Year",
    a.buzei                                     AS "Line Item",
    a.blart                                     AS "Doc. Type",

    CASE a.blart
        WHEN 'DR' THEN 'Customer Invoice'
        WHEN 'DG' THEN 'Customer Credit Memo'
        WHEN 'DZ' THEN 'Customer Payment'
        WHEN 'AB' THEN 'Accounting Document'
        WHEN 'SA' THEN 'G/L Account Document'
        WHEN 'KR' THEN 'Vendor Invoice'
        WHEN 'ZP' THEN 'Payment Posting'
        ELSE a.blart
    END                                         AS "Doc. Type Text",

    /* ── Customer ────────────────────────────────────────────── */
    a.kunnr                                     AS "Account",
    COALESCE(NULLIF(kna.name1,''), '— not found —')
                                                AS "Customer Name",
    kna.name2                                   AS "Customer Name 2",
    kna.ort01                                   AS "City",
    kna.land1                                   AS "Country",
    kna.ktokd                                   AS "Account Group",

    /* ── Reference & assignment ──────────────────────────────── */
    a.xblnr                                     AS "Reference",
    a.zuonr                                     AS "Assignment",
    a.sgtxt                                     AS "Text",
    a.rebzg                                     AS "Invoice Ref.",
    a.rebzj                                     AS "Invoice Ref. Year",

    /* ── G/L & special indicators ────────────────────────────── */
    a.hkont                                     AS "G/L Account",
    a.umskz                                     AS "Special G/L Ind.",
    a.bschl                                     AS "Posting Key",
    a.shkzg                                     AS "Dr/Cr Indicator",
    a.dr_cr_text                                AS "Dr/Cr Text",

    /* ── Dates ───────────────────────────────────────────────── */
    a.posting_date                              AS "Pstng Date",
    a.document_date                             AS "Doc. Date",
    a.baseline_date                             AS "Baseline Date",
    a.net_due_date                              AS "Net Due Date",
    a.clearing_date                             AS "Clearing Date",
    a.augbl                                     AS "Clearing Document",

    /* ── Amounts ─────────────────────────────────────────────── */
    a.amt_doc_curr_signed                       AS "Amount in DC",
    a.waers                                     AS "Curr.",
    a.amt_local_curr_signed                     AS "Amt in LC",

    /* Absolute raw amounts with Currency Factor applied */
    CAST(NULLIF(a.wrbtr,'') AS NUMERIC(18,2)) *
    (CASE WHEN a.waers IN ('UGX', 'RWF') THEN 100.0 ELSE 1.0 END)
                                                AS "Abs. Amt in DC",

    CAST(NULLIF(a.dmbtr,'') AS NUMERIC(18,2)) *
    (CASE WHEN a.waers IN ('UGX', 'RWF') THEN 100.0 ELSE 1.0 END)
                                                AS "Abs. Amt in LC",

    /* ── Aging ───────────────────────────────────────────────── */
    a.days_overdue                              AS "Days Overdue",
    a.aging_bucket                              AS "Aging Bucket",

    /* ── Payment terms & controls ────────────────────────────── */
    a.zterm                                     AS "Pmnt Terms",
    CAST(NULLIF(a.zbd3t,'') AS INTEGER)         AS "Net Days",
    a.zlsch                                     AS "Pmnt Method",
    a.zlspr                                     AS "Pmnt Block",

    /* ── Tax & org fields ────────────────────────────────────── */
    a.mwskz                                     AS "Tax Code",
    a.gsber                                     AS "Business Area",
    a.kostl                                     AS "Cost Center",
    a.prctr                                     AS "Profit Center",

    /* ── SD link ─────────────────────────────────────────────── */
    a.vbeln                                     AS "Billing Doc.",

    /* ── Year / period derived ───────────────────────────────── */
    EXTRACT(YEAR FROM a.posting_date)           AS "Year",
    TO_CHAR(a.posting_date, 'YYYY/MM')          AS "Year/Month"

FROM CTE_AGING a
LEFT JOIN "sap_stg_ecc"."stg_sap_fbl5n_kna1" kna
    ON kna.kunnr = a.kunnr

ORDER BY
    a.kunnr         ASC,
    a.posting_date  DESC,
    a.belnr         ASC,
    a.buzei         ASC;

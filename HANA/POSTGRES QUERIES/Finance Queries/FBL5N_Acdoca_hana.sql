/* ============================================================
   FBL5N — Customer Line Items  |  SAP S/4HANA (ACDOCA) version
   ============================================================
   Source : sap_stg_hana.stg_sap_acdoca_acdoca  (KOART = 'D')
            sap_stg_hana.stg_sap_acdoca_kna1
   Output : same 47 columns, names, order and data types as
            fbl5n_ecc_postgres.sql.

   ECC → ACDOCA differences handled here
     • Open / Cleared   : from AUGBL (blank = open), not BSID/BSAD.
     • Amounts          : HSL / WSL already signed; trailing minus parsed.
     • Currency factor  : UGX / RWF × 100 per amount's own currency
                          (HSL → RHCUR, WSL → RWCUR).
     • Net due date     : ACDOCA.NETDT (SAP's own calculation).
     • Billing Doc.     : AWREF where AWTYP = 'VBRK' (ECC BSID.VBELN).
     • Not in ACDOCA → NULL until BKPF / BSEG are staged:
         Reference (XBLNR, BKPF), Baseline Date (ZFBDT), Pmnt Terms
         (ZTERM), Net Days (ZBD3T), Pmnt Method (ZLSCH), Pmnt Block
         (ZLSPR) — all BSEG.
   ============================================================ */

WITH CTE_LINE_ITEMS AS (

    SELECT
        CASE WHEN NULLIF(a.augbl, '') IS NULL
             THEN 'Open' ELSE 'Cleared' END      AS item_status,
        a.rbukrs                                 AS bukrs,
        a.kunnr, a.belnr, a.gjahr, a.buzei,
        a.budat, a.bldat, a.blart, a.zuonr,
        a.racct                                  AS hkont,
        a.umskz, a.bschl,
        a.drcrk                                  AS shkzg,
        a.rwcur                                  AS waers,
        a.sgtxt, a.rebzg, a.rebzj, a.netdt,
        a.rbusa                                  AS gsber,
        a.mwskz,
        CASE WHEN a.awtyp = 'VBRK' THEN a.awref END AS vbeln,
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
      AND a.koart = 'D'                          -- customer lines only
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
        NULL::DATE                                     AS baseline_date,   -- ZFBDT: BSEG, not staged
        CASE WHEN NULLIF(NULLIF(li.augdt, ''), '00000000') IS NOT NULL
             THEN TO_DATE(li.augdt, 'YYYYMMDD')
        END                                            AS clearing_date,

        /* ── Net Due Date = ACDOCA.NETDT ─────────────────────── */
        CASE WHEN NULLIF(NULLIF(li.netdt, ''), '00000000') IS NOT NULL
             THEN TO_DATE(li.netdt, 'YYYYMMDD')
        END                                            AS net_due_date,

        /* ── Signed amounts ──────────────────────────────────── */
        ROUND(li.amt_doc_curr, 2)       AS amt_doc_curr_signed,
        ROUND(li.amt_local_curr, 2)       AS amt_local_curr_signed,

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
    MAIN SELECT (47 columns)
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
    NULL::TEXT                                  AS "Reference",   -- XBLNR: BKPF, not staged yet
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

    /* Absolute amounts with currency factor applied */
    ABS(a.amt_doc_curr_signed)                  AS "Abs. Amt in DC",
    ABS(a.amt_local_curr_signed)                AS "Abs. Amt in LC",

    /* ── Aging ───────────────────────────────────────────────── */
    a.days_overdue                              AS "Days Overdue",
    a.aging_bucket                              AS "Aging Bucket",

    /* ── Payment terms & controls (BSEG — not staged yet) ────── */
    NULL::TEXT                                  AS "Pmnt Terms",
    NULL::INTEGER                               AS "Net Days",
    NULL::TEXT                                  AS "Pmnt Method",
    NULL::TEXT                                  AS "Pmnt Block",

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
LEFT JOIN "sap_stg_hana"."stg_sap_acdoca_kna1" kna
    ON kna.kunnr = a.kunnr

ORDER BY
    a.kunnr         ASC,
    a.posting_date  DESC,
    a.belnr         ASC,
    a.buzei         ASC;
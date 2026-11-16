/* ============================================================
   FBL1N — Vendor Line Items  |  SAP S/4HANA (ACDOCA) version
   ============================================================
   Source : sap_stg_hana.stg_sap_acdoca_acdoca  (KOART = 'K')
            sap_stg_hana.stg_sap_acdoca_lfa1
   Output : same 27 columns, names, order and data types as
            fbl1n_ecc_postgres_updated.sql (v1.3).

   ECC → ACDOCA differences handled here
     • Open / Cleared   : from AUGBL (blank = open), not BSIK/BSAK.
     • Amounts          : HSL / WSL are already signed in ACDOCA
                          (credit = negative); RFC delivers negatives
                          with a trailing minus ("1234.56-").
     • Currency factor  : UGX / RWF × 100, applied per amount's own
                          currency (HSL → RHCUR, WSL → RWCUR).
     • Net due date     : ACDOCA.NETDT (SAP's own calculation).
     • Clrng doc./Cleared: from AUGBL (ECC version used REBZG).
     • Reference (XBLNR): not in ACDOCA → NULL until BKPF is staged.
   ============================================================ */

WITH cte_all_items AS (

    SELECT
        a.rbukrs                                    AS bukrs,
        a.lifnr, a.belnr, a.gjahr, a.buzei,
        a.budat, a.bldat, a.blart, a.zuonr,
        a.racct                                     AS hkont,
        a.sgtxt, a.rebzg,
        a.netdt, a.augdt, a.augbl,
        a.drcrk                                     AS shkzg,   -- S = Debit, H = Credit
        a.rwcur                                     AS waers,
        CASE WHEN NULLIF(a.augbl, '') IS NULL
             THEN 'Open' ELSE 'Cleared' END         AS item_status,

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
    FROM sap_stg_hana.stg_sap_acdoca_acdoca a
    WHERE a.rldnr = '0L'
      AND a.koart = 'K'                             -- vendor (supplier) lines only

),

cte_vendor_master AS (
    SELECT
        lifnr,
        name1   AS vendor_name,
        name2   AS vendor_name2,
        ort01   AS city,
        land1   AS country,
        stras   AS street,
        pstlz   AS postal_code,
        ktokk   AS account_group
    FROM sap_stg_hana.stg_sap_acdoca_lfa1
),

cte_computed AS (
    SELECT
        li.*,
        ROUND(li.amt_doc_curr, 2)    AS amt_doc_curr_signed,
        ROUND(li.amt_local_curr, 2)    AS amt_local_curr_signed,
        CASE li.shkzg
            WHEN 'S' THEN 'Debit'
            WHEN 'H' THEN 'Credit'
            ELSE li.shkzg
        END                                         AS dr_cr_text,
        CASE WHEN NULLIF(NULLIF(li.augbl, ''), '0000000000') IS NOT NULL
             THEN 'Y' ELSE 'N' END                  AS is_cleared,
        NULLIF(li.augbl, '')                        AS clearing_doc,
        CASE WHEN NULLIF(NULLIF(li.netdt, ''), '00000000') IS NOT NULL
             THEN TO_DATE(li.netdt, 'YYYYMMDD') END AS net_due_date,
        NULLIF(NULLIF(li.augdt, ''), '00000000')    AS actual_clearing_date
    FROM cte_all_items li
)

/* ──────────────────────────────────────────────────────────────
   MAIN SELECT — FBL1N Report Columns (27)
   ────────────────────────────────────────────────────────────── */
SELECT

    /* ── Document Identification ────────────────────────────── */
    li.bukrs                                            AS "CoCd",
    li.belnr                                            AS "Doc. No.",
    li.gjahr                                            AS "Year",
    li.buzei                                            AS "Itm",

    /* ── Vendor ─────────────────────────────────────────────── */
    li.lifnr                                            AS "Vendor",
    COALESCE(NULLIF(vm.vendor_name, ''), '— vendor not in master —') AS "Vendor Name",

    /* ── Dates & Due Date ───────────────────────────────────── */
    li.budat                                            AS "Pstng Date",
    li.bldat                                            AS "Doc. Date",
    li.net_due_date                                     AS "Net due date",

    CASE
        WHEN li.item_status = 'Open' AND li.net_due_date < CURRENT_DATE THEN 'Overdue'
        WHEN li.item_status = 'Open'                                    THEN 'Pending'
        ELSE 'Settled'
    END                                                 AS "Status Ind.",

    /* ── Document Type ──────────────────────────────────────── */
    li.blart                                            AS "Doc.Type",
    CASE li.blart
        WHEN 'KR' THEN 'Vendor Invoice'
        WHEN 'RE' THEN 'Invoice Receipt (LIV)'
        WHEN 'KG' THEN 'Vendor Credit Memo'
        WHEN 'KZ' THEN 'Vendor Payment'
        WHEN 'ZP' THEN 'Payment Posting'
        WHEN 'AB' THEN 'Accounting Document'
        ELSE li.blart
    END                                                 AS "Document Type Text",

    /* ── References ─────────────────────────────────────────── */
    NULL::TEXT                                          AS "Reference",   -- XBLNR: BKPF, not staged yet
    li.zuonr                                            AS "Assignment",
    li.hkont                                            AS "G/L Acct",
    li.sgtxt                                            AS "Text",

    /* ── Debit / Credit Indicator ───────────────────────────── */
    li.shkzg                                            AS "Dr/Cr Indicator",
    li.dr_cr_text                                       AS "Dr/Cr Text",

    /* ── Signed Amounts (already signed in ACDOCA) ──────────── */
    li.amt_doc_curr_signed                              AS "Amount",
    li.waers                                            AS "Curr.",
    li.amt_local_curr_signed                            AS "Amt.in LC",

    /* ── Absolute Amounts (for reconciliation) ──────────────── */
    CAST(ABS(li.amt_doc_curr_signed) AS NUMERIC(18,2))   AS "Amount (Abs.)",
    CAST(ABS(li.amt_local_curr_signed) AS NUMERIC(18,2)) AS "Amt.in LC (Abs.)",

    /* ── Item Status & Clearing ─────────────────────────────── */
    li.item_status                                      AS "Item Status",
    li.is_cleared                                       AS "Cleared",
    li.clearing_doc                                     AS "Clrng doc.",
    li.actual_clearing_date                             AS "Clrng Date"

FROM      cte_computed       li
LEFT JOIN cte_vendor_master  vm ON vm.lifnr = li.lifnr

ORDER BY
    li.budat    DESC,
    li.lifnr    ASC,
    li.belnr    ASC;
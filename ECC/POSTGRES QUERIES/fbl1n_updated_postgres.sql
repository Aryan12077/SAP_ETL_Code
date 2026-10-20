/* ============================================================
   FBL1N — Vendor Line Items  |  Replication Query (v1.2)
   ============================================================
   Updated: 2026-05-12
   Additions: Net Due Date calculation and Clearing Date.
   ============================================================ */

WITH cte_all_items AS (

    SELECT
        bukrs, lifnr, belnr, gjahr, buzei,
        budat, bldat, blart, xblnr, zuonr,
        hkont, wrbtr, waers, dmbtr, sgtxt, rebzg,
        zfbdt, zbd1t, 
        NULL AS augdt,             -- Clearing date only exists in BSAK
        'Open'    AS item_status
    FROM sap_stg_ecc.stg_sap_fbl1n_v2_bsik

    UNION ALL

    SELECT
        bukrs, lifnr, belnr, gjahr, buzei,
        budat, bldat, blart, xblnr, zuonr,
        hkont, wrbtr, waers, dmbtr, sgtxt, rebzg,
        zfbdt, zbd1t,
        augdt,                     -- Available for cleared items
        'Cleared' AS item_status
    FROM sap_stg_ecc.stg_sap_fbl1n_v2_bsak

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
    FROM sap_stg_ecc.stg_sap_fbl1n_v2_lfa1
),

cte_clearing_status AS (
    SELECT
        belnr, gjahr, bukrs, buzei,
        item_status,
        CASE
            WHEN item_status = 'Cleared'         THEN 'Y'
            WHEN NULLIF(rebzg, '') IS NOT NULL  THEN 'Y'
            ELSE 'N'
        END                                     AS is_cleared,
        NULLIF(rebzg, '')                       AS clearing_doc,
        /* 
           CALCULATION: Net Due Date 
           Baseline Date (zfbdt) + Cash Discount Days (zbd1t)
        */
        CASE 
            WHEN NULLIF(zfbdt, '00000000') IS NOT NULL 
            THEN (TO_DATE(zfbdt, 'YYYYMMDD') + (COALESCE(NULLIF(zbd1t, ''), '0')::INTEGER * INTERVAL '1 day'))::DATE
            ELSE NULL 
        END                                     AS net_due_date,
        NULLIF(augdt, '00000000')               AS actual_clearing_date
    FROM cte_all_items
)

/* ──────────────────────────────────────────────────────────────
   MAIN SELECT — FBL1N Report Columns
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
    cs.net_due_date                                     AS "Net due date",
    
    /* Show 'Delayed' icon logic similar to SAP */
    CASE 
        WHEN li.item_status = 'Open' AND cs.net_due_date < CURRENT_DATE THEN 'Overdue'
        WHEN li.item_status = 'Open' THEN 'Pending'
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
    li.xblnr                                            AS "Reference",
    li.zuonr                                            AS "Assignment",
    li.hkont                                            AS "G/L Acct",
    li.sgtxt                                            AS "Text",

    /* ── Amounts ────────────────────────────────────────────── */
    CAST(li.wrbtr AS NUMERIC(18,2))                     AS "Amount",
    li.waers                                            AS "Curr.",
    CAST(li.dmbtr AS NUMERIC(18,2))                     AS "Amt.in LC",

    /* ── Item Status & Clearing ──────────────────────────────── */
    li.item_status                                      AS "Item Status",
    cs.is_cleared                                       AS "Cleared",
    cs.clearing_doc                                     AS "Clrng doc.",
    cs.actual_clearing_date                             AS "Clrng Date"

FROM      cte_all_items                             li
LEFT JOIN cte_vendor_master                         vm ON vm.lifnr = li.lifnr
LEFT JOIN cte_clearing_status                       cs ON cs.belnr = li.belnr 
                                                       AND cs.gjahr = li.gjahr 
                                                       AND cs.bukrs = li.bukrs 
                                                       AND cs.buzei = li.buzei

ORDER BY
    li.budat    DESC,
    li.lifnr    ASC,
    li.belnr    ASC;
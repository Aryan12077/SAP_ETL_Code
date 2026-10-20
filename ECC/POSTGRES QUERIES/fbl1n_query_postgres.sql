/* ============================================================
   FBL1N — Vendor Line Items  |  Replication Query
   ============================================================
   Staging tables used (schema: sap_stg_ecc):
     stg_sap_fbl1n_bsik  — Vendor Open Items
     stg_sap_fbl1n_bsak  — Vendor Cleared Items (paid/cleared)
     stg_sap_fbl1n_lfa1  — Vendor Master (Name, Address)

   All column names are lowercase as written by the Python loader.

   FBL1N Report Column → Staging Table Field Mapping:
   ─────────────────────────────────────────────────────
   CoCd         (Company Code)            → bsik/bsak.bukrs
   Vendor       (Vendor Account)          → bsik/bsak.lifnr
   Doc. No.     (Accounting Document)     → bsik/bsak.belnr
   Year         (Fiscal Year)             → bsik/bsak.gjahr
   Itm          (Line Item)               → bsik/bsak.buzei
   Pstng Date   (Posting Date)            → bsik/bsak.budat
   Doc. Date    (Document Date)           → bsik/bsak.bldat
   Doc.Type     (Document Type)           → bsik/bsak.blart
   Reference    (Reference Document)      → bsik/bsak.xblnr
   Assignment   (Assignment Number)       → bsik/bsak.zuonr
   G/L Acct     (G/L Account)            → bsik/bsak.hkont
   Amount       (Amount in Doc. Currency) → bsik/bsak.wrbtr
   Curr.        (Document Currency)       → bsik/bsak.waers
   Amt.in LC    (Amount in Local Curr.)   → bsik/bsak.dmbtr
   Text         (Item Text)               → bsik/bsak.sgtxt
   Clrng doc.   (Clearing Document)       → bsik/bsak.rebzg

   BLART Common Document Type Codes:
     'KR' = Vendor Invoice
     'RE' = Invoice Receipt (Logistics)
     'KG' = Vendor Credit Memo
     'KZ' = Vendor Payment
     'ZP' = Payment Posting
     'AB' = Accounting Document

   Item Status (derived):
     'Open'    = row is from BSIK (not yet cleared)
     'Cleared' = row is from BSAK (cleared/paid)

   Run this after the Python extraction completes successfully.
   ============================================================ */


/* ──────────────────────────────────────────────────────────────
   STEP 1  —  CTE: Combine open and cleared items into one set
   BSIK holds open (unpaid) items; BSAK holds cleared (paid)
   items. Both have identical columns so a UNION ALL gives the
   full vendor line-item picture, equivalent to running FBL1N
   with "Open items" + "Cleared items" both ticked.
   Each row is tagged with its item_status for downstream
   filtering.
   ────────────────────────────────────────────────────────────── */
WITH cte_all_items AS (

    SELECT
        bukrs, lifnr, belnr, gjahr, buzei,
        budat, bldat, blart, xblnr, zuonr,
        hkont, wrbtr, waers, dmbtr, sgtxt, rebzg,
        'Open'    AS item_status
    FROM sap_stg_ecc.stg_sap_fbl1n_bsik

    UNION ALL

    SELECT
        bukrs, lifnr, belnr, gjahr, buzei,
        budat, bldat, blart, xblnr, zuonr,
        hkont, wrbtr, waers, dmbtr, sgtxt, rebzg,
        'Cleared' AS item_status
    FROM sap_stg_ecc.stg_sap_fbl1n_bsak

),

/* ──────────────────────────────────────────────────────────────
   STEP 2  —  CTE: Vendor name and address from LFA1
   Join on lifnr for the primary vendor details.
   Mirrors exactly how VF05N joins CTE_SOLDTO_NAME from KNA1.
   ────────────────────────────────────────────────────────────── */
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
    FROM sap_stg_ecc.stg_sap_fbl1n_lfa1
),

/* ──────────────────────────────────────────────────────────────
   STEP 3  —  CTE: Clearing status
   Derives a clean cleared flag from the item_status tag and
   the rebzg (clearing document) field.
   Mirrors the CTE_CANCEL_STATUS pattern in the VF05N query.
   ────────────────────────────────────────────────────────────── */
cte_clearing_status AS (
    SELECT
        belnr,
        gjahr,
        bukrs,
        buzei,
        item_status,
        CASE
            WHEN item_status = 'Cleared'        THEN 'Y'
            WHEN NULLIF(rebzg, '') IS NOT NULL  THEN 'Y'
            ELSE 'N'
        END                         AS is_cleared,
        NULLIF(rebzg, '')           AS clearing_doc
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
    COALESCE(
        NULLIF(vm.vendor_name, ''),
        '— vendor not in master —'
    )                                                   AS "Vendor Name",
    vm.vendor_name2                                     AS "Vendor Name 2",
    vm.city                                             AS "City",
    vm.country                                          AS "Country",
    vm.street                                           AS "Street",
    vm.postal_code                                      AS "Postal Code",

    /* ── Dates ──────────────────────────────────────────────── */
    li.budat                                            AS "Pstng Date",
    li.bldat                                            AS "Doc. Date",

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

    /* ── Item Status ────────────────────────────────────────── */
    li.item_status                                      AS "Item Status",
    cs.is_cleared                                       AS "Cleared",
    cs.clearing_doc                                     AS "Clrng doc."

FROM      cte_all_items                             li

/* ── Vendor master name and address ─────────────────────────── */
LEFT JOIN cte_vendor_master                         vm
    ON  vm.lifnr = li.lifnr

/* ── Clearing status ────────────────────────────────────────── */
LEFT JOIN cte_clearing_status                       cs
    ON  cs.belnr  = li.belnr
    AND cs.gjahr  = li.gjahr
    AND cs.bukrs  = li.bukrs
    AND cs.buzei  = li.buzei

ORDER BY
    li.budat    DESC,       -- most recent posting dates first
    li.lifnr    ASC,
    li.belnr    ASC,
    li.buzei    ASC;


/* ============================================================
   SUMMARY VIEWS
   Run these separately for aggregated analysis.
   ============================================================ */

/* -- S1: Payables summary by document type and company code -- */
/*
SELECT
    li.bukrs                                        AS "CoCd",
    li.blart                                        AS "Doc.Type",
    li.waers                                        AS "Curr.",
    li.item_status                                  AS "Item Status",
    COUNT(*)                                        AS "Line Count",
    SUM(CAST(li.wrbtr AS NUMERIC(18,2)))            AS "Total Amount",
    SUM(CAST(li.dmbtr AS NUMERIC(18,2)))            AS "Total LC Amount"
FROM (
    SELECT bukrs, blart, waers, wrbtr, dmbtr, 'Open'    AS item_status FROM sap_stg_ecc.stg_sap_fbl1n_bsik
    UNION ALL
    SELECT bukrs, blart, waers, wrbtr, dmbtr, 'Cleared' AS item_status FROM sap_stg_ecc.stg_sap_fbl1n_bsak
) li
GROUP BY li.bukrs, li.blart, li.waers, li.item_status
ORDER BY "Total Amount" DESC;
*/

/* -- S2: Daily posting activity ----------------------------- */
/*
SELECT
    li.budat                                        AS "Pstng Date",
    li.bukrs                                        AS "CoCd",
    li.blart                                        AS "Doc.Type",
    li.item_status                                  AS "Item Status",
    COUNT(*)                                        AS "Line Items",
    SUM(CAST(li.wrbtr AS NUMERIC(18,2)))            AS "Total Amount"
FROM (
    SELECT bukrs, budat, blart, wrbtr, 'Open'    AS item_status FROM sap_stg_ecc.stg_sap_fbl1n_bsik
    UNION ALL
    SELECT bukrs, budat, blart, wrbtr, 'Cleared' AS item_status FROM sap_stg_ecc.stg_sap_fbl1n_bsak
) li
GROUP BY li.budat, li.bukrs, li.blart, li.item_status
ORDER BY li.budat DESC;
*/

/* -- S3: Payables by vendor ---------------------------------- */
/*
SELECT
    li.lifnr                                        AS "Vendor",
    lf.name1                                        AS "Vendor Name",
    li.waers                                        AS "Curr.",
    li.item_status                                  AS "Item Status",
    COUNT(*)                                        AS "Line Count",
    SUM(CAST(li.wrbtr AS NUMERIC(18,2)))            AS "Total Amount"
FROM (
    SELECT lifnr, waers, wrbtr, 'Open'    AS item_status FROM sap_stg_ecc.stg_sap_fbl1n_bsik
    UNION ALL
    SELECT lifnr, waers, wrbtr, 'Cleared' AS item_status FROM sap_stg_ecc.stg_sap_fbl1n_bsak
) li
LEFT JOIN sap_stg_ecc.stg_sap_fbl1n_lfa1 lf ON lf.lifnr = li.lifnr
GROUP BY li.lifnr, lf.name1, li.waers, li.item_status
ORDER BY "Total Amount" DESC;
*/

/* -- S4: Open items (unpaid invoices) only ------------------- */
/*
SELECT
    bsik.bukrs                                      AS "CoCd",
    bsik.lifnr                                      AS "Vendor",
    lf.name1                                        AS "Vendor Name",
    bsik.belnr                                      AS "Doc. No.",
    bsik.gjahr                                      AS "Year",
    bsik.budat                                      AS "Pstng Date",
    bsik.bldat                                      AS "Doc. Date",
    bsik.blart                                      AS "Doc.Type",
    bsik.xblnr                                      AS "Reference",
    CAST(bsik.wrbtr AS NUMERIC(18,2))               AS "Amount",
    bsik.waers                                      AS "Curr.",
    CAST(bsik.dmbtr AS NUMERIC(18,2))               AS "Amt.in LC"
FROM sap_stg_ecc.stg_sap_fbl1n_bsik bsik
LEFT JOIN sap_stg_ecc.stg_sap_fbl1n_lfa1 lf ON lf.lifnr = bsik.lifnr
ORDER BY bsik.budat ASC;   -- oldest first (highest overdue risk)
*/

/* -- S5: Cleared items (paid invoices) only ----------------- */
/*
SELECT
    bsak.bukrs                                      AS "CoCd",
    bsak.lifnr                                      AS "Vendor",
    lf.name1                                        AS "Vendor Name",
    bsak.belnr                                      AS "Doc. No.",
    bsak.gjahr                                      AS "Year",
    bsak.budat                                      AS "Pstng Date",
    CAST(bsak.wrbtr AS NUMERIC(18,2))               AS "Amount",
    bsak.waers                                      AS "Curr.",
    NULLIF(bsak.rebzg, '')                          AS "Clrng doc."
FROM sap_stg_ecc.stg_sap_fbl1n_bsak bsak
LEFT JOIN sap_stg_ecc.stg_sap_fbl1n_lfa1 lf ON lf.lifnr = bsak.lifnr
ORDER BY bsak.budat DESC;
*/

/* -- S6: G/L account distribution (spend by account) -------- */
/*
SELECT
    li.bukrs                                        AS "CoCd",
    li.hkont                                        AS "G/L Acct",
    li.waers                                        AS "Curr.",
    COUNT(*)                                        AS "Line Count",
    SUM(CAST(li.wrbtr AS NUMERIC(18,2)))            AS "Total Amount"
FROM (
    SELECT bukrs, hkont, waers, wrbtr FROM sap_stg_ecc.stg_sap_fbl1n_bsik
    UNION ALL
    SELECT bukrs, hkont, waers, wrbtr FROM sap_stg_ecc.stg_sap_fbl1n_bsak
) li
GROUP BY li.bukrs, li.hkont, li.waers
ORDER BY "Total Amount" DESC;
*/


/* ============================================================
   VALIDATION QUERIES
   Run these separately to check data quality after the main
   query looks correct.
   ============================================================ */

/* -- V1: Row count check ------------------------------------ */
-- SELECT COUNT(*) FROM sap_stg_ecc.stg_sap_fbl1n_bsik;
-- SELECT COUNT(*) FROM sap_stg_ecc.stg_sap_fbl1n_bsak;
-- SELECT COUNT(*) FROM sap_stg_ecc.stg_sap_fbl1n_lfa1;

/* -- V2: Vendors not resolved in LFA1 ----------------------- */
-- SELECT DISTINCT li.lifnr
-- FROM (
--     SELECT lifnr FROM sap_stg_ecc.stg_sap_fbl1n_bsik
--     UNION
--     SELECT lifnr FROM sap_stg_ecc.stg_sap_fbl1n_bsak
-- ) li
-- LEFT JOIN sap_stg_ecc.stg_sap_fbl1n_lfa1 lf ON lf.lifnr = li.lifnr
-- WHERE lf.lifnr IS NULL
-- ORDER BY li.lifnr;

/* -- V3: Distinct document types in the extract ------------- */
-- SELECT blart, COUNT(*) AS cnt
-- FROM (
--     SELECT blart FROM sap_stg_ecc.stg_sap_fbl1n_bsik
--     UNION ALL
--     SELECT blart FROM sap_stg_ecc.stg_sap_fbl1n_bsak
-- ) t
-- GROUP BY blart ORDER BY cnt DESC;

/* -- V4: Open vs cleared item count and value breakdown ----- */
-- SELECT
--     'Open'    AS item_status,
--     COUNT(*)  AS line_count,
--     SUM(CAST(wrbtr AS NUMERIC(18,2))) AS total_amount
-- FROM sap_stg_ecc.stg_sap_fbl1n_bsik
-- UNION ALL
-- SELECT
--     'Cleared' AS item_status,
--     COUNT(*)  AS line_count,
--     SUM(CAST(wrbtr AS NUMERIC(18,2))) AS total_amount
-- FROM sap_stg_ecc.stg_sap_fbl1n_bsak;

/* -- V5: Amount range check (look for outliers) ------------- */
-- SELECT
--     MIN(CAST(wrbtr AS NUMERIC(18,2))) AS min_amount,
--     MAX(CAST(wrbtr AS NUMERIC(18,2))) AS max_amount,
--     AVG(CAST(wrbtr AS NUMERIC(18,2))) AS avg_amount,
--     COUNT(*)                          AS total_lines
-- FROM (
--     SELECT wrbtr FROM sap_stg_ecc.stg_sap_fbl1n_bsik
--     UNION ALL
--     SELECT wrbtr FROM sap_stg_ecc.stg_sap_fbl1n_bsak
-- ) t;

/* -- V6: Duplicate line item check (PK: bukrs+belnr+gjahr+buzei) */
-- SELECT bukrs, belnr, gjahr, buzei, COUNT(*) AS cnt
-- FROM sap_stg_ecc.stg_sap_fbl1n_bsik
-- GROUP BY bukrs, belnr, gjahr, buzei
-- HAVING COUNT(*) > 1;
--
-- SELECT bukrs, belnr, gjahr, buzei, COUNT(*) AS cnt
-- FROM sap_stg_ecc.stg_sap_fbl1n_bsak
-- GROUP BY bukrs, belnr, gjahr, buzei
-- HAVING COUNT(*) > 1;

/* -- V7: Distinct G/L accounts in the extract --------------- */
-- SELECT hkont, COUNT(*) AS cnt
-- FROM (
--     SELECT hkont FROM sap_stg_ecc.stg_sap_fbl1n_bsik
--     UNION ALL
--     SELECT hkont FROM sap_stg_ecc.stg_sap_fbl1n_bsak
-- ) t
-- GROUP BY hkont ORDER BY cnt DESC;
/* ============================================================
    VF05N — List of Billing Documents | Modified for UGX/RWF Scaling
   ============================================================ */

WITH CTE_SOLDTO_NAME AS (
    SELECT
        kunnr,
        name1                   AS customer_name,
        name2                   AS customer_name2,
        ort01                   AS city,
        land1                   AS country
    FROM "sap_stg_ecc"."stg_sap_vf05n_kna1"
),

CTE_CANCEL_STATUS AS (
    SELECT
        vbeln,
        CASE
            WHEN fksto = 'X' THEN 'Y'
            ELSE 'N'
        END                     AS is_cancelled,
        NULLIF(sfakn, '')       AS cancelled_by_doc
    FROM "sap_stg_ecc"."stg_sap_vf05n_vbrk"
)

/* ──────────────────────────────────────────────────────────────
    MAIN SELECT
   ────────────────────────────────────────────────────────────── */
SELECT

    /* ── Billing Document Identification ───────────────────── */
    vb.vbeln                                        AS "Bill.Doc.",
    vb.fkart                                        AS "BillT",

    CASE vb.fkart
        WHEN 'ZF29' THEN 'Invoice ZF29'
        WHEN 'ZF2A' THEN 'Invoice ZF2A'
        WHEN 'ZRE'  THEN 'Returns Credit'
        WHEN 'ZG2'  THEN 'Credit Memo'
        WHEN 'ZL2'  THEN 'Debit Memo'
        ELSE vb.fkart
    END                                             AS "Billing Type Text",

    vb.fktyp                                        AS "BlCat",
    CASE vb.fktyp
        WHEN 'L' THEN 'Standard Invoice'
        WHEN 'B' THEN 'Invoice List'
        WHEN 'S' THEN 'Cancellation'
        ELSE vb.fktyp
    END                                             AS "Billing Category Text",

    vb.vbtyp                                        AS "DocCa",
    CASE vb.vbtyp
        WHEN 'M' THEN 'Invoice'
        WHEN 'N' THEN 'Credit Memo'
        WHEN 'O' THEN 'Debit Memo'
        WHEN 'S' THEN 'Cancellation Invoice'
        ELSE vb.vbtyp
    END                                             AS "Doc Category Text",

    /* ── Dates ──────────────────────────────────────────────── */
    vb.fkdat                                        AS "Bill. Date",
    vb.erdat                                        AS "Created On",
    vb.erzet                                        AS "Created At",

    /* ── Organisation ───────────────────────────────────────── */
    vb.bukrs                                        AS "Company Code",
    vb.vkorg                                        AS "SOrg.",
    vb.vtweg                                        AS "DChl",
    vb.spart                                        AS "Division",

    /* ── Customer ───────────────────────────────────────────── */
    vb.kunag                                        AS "Sold-to pt",
    COALESCE(
        NULLIF(sn.customer_name, ''),
        '— customer not in master —'
    )                                               AS "Customer Name",
    sn.city                                         AS "City",
    sn.country                                      AS "Country",
    vb.kunrg                                        AS "Payer",

    /* ── Value with UGX/RWF Scaling ────────────────────────── */
    CASE
        WHEN vb.waerk IN ('UGX', 'RWF')
        THEN CAST(NULLIF(vb.netwr, '') AS NUMERIC(18,2)) * 100
        ELSE CAST(NULLIF(vb.netwr, '') AS NUMERIC(18,2))
    END                                             AS "Net Value",

    vb.waerk                                        AS "Curr.",

    CASE
        WHEN vb.waerk IN ('UGX', 'RWF')
        THEN CAST(NULLIF(vb.mwsbk, '') AS NUMERIC(18,2)) * 100
        ELSE CAST(NULLIF(vb.mwsbk, '') AS NUMERIC(18,2))
    END                                             AS "Tax amount",

    /* Gross value derived */
    CASE
        WHEN vb.waerk IN ('UGX', 'RWF')
        THEN (CAST(NULLIF(vb.netwr, '') AS NUMERIC(18,2))
            + CAST(NULLIF(vb.mwsbk, '') AS NUMERIC(18,2))) * 100
        ELSE CAST(NULLIF(vb.netwr, '') AS NUMERIC(18,2))
            + CAST(NULLIF(vb.mwsbk, '') AS NUMERIC(18,2))
    END                                             AS "Gross Value",

    /* ── Posting Status ─────────────────────────────────────── */
    vb.rfbsk                                        AS "PsSt",
    CASE vb.rfbsk
        WHEN 'A' THEN 'Not yet posted'
        WHEN 'B' THEN 'Partially transferred'
        WHEN 'C' THEN 'Fully posted'
        WHEN 'D' THEN 'Cleared in FI'
        ELSE          'Unknown'
    END                                             AS "Posting Status Text",

    /* ── Accounting Reference ───────────────────────────────── */
    vb.belnr                                        AS "Accounting Doc.",
    vb.gjahr                                        AS "Fiscal Year",

    /* ── Cancellation ───────────────────────────────────────── */
    cs.is_cancelled                                 AS "Cancelled",
    cs.cancelled_by_doc                             AS "Cancellation Doc",

    /* ── Reference ──────────────────────────────────────────── */
    vb.bstnk_vf                                     AS "Customer PO",
    vb.zterm                                        AS "Payment Terms",

    /* ── Audit ──────────────────────────────────────────────── */
    vb.ernam                                        AS "Created"

FROM      "sap_stg_ecc"."stg_sap_vf05n_vbrk"       vb

LEFT  JOIN CTE_SOLDTO_NAME            sn
    ON  sn.kunnr = vb.kunag

LEFT  JOIN CTE_CANCEL_STATUS          cs
    ON  cs.vbeln = vb.vbeln

ORDER BY
    vb.fkdat   DESC,
    vb.vbeln   ASC;


-- SAP Reference IMG
-- │
-- ├── Enterprise Structure
-- │
-- ├── Financial Accounting
-- │
-- ├── Controlling
-- │
-- ├── Sales and Distribution
-- │
-- ├── Materials Management
-- │
-- ├── Production
-- │
-- ├── Plant Maintenance
-- │
-- └── Human Resources


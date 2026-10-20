/* ============================================================
   VF05N — List of Billing Documents | Modified for UGX/RWF Scaling
   Extended with Item-Level Fields:
     Sale Quantity  → VBRP.FKIMG
     COGS           → VBRP.WAVWR   (scaled for UGX/RWF)
     COGS_MBEW      → VBRP.FKIMG * MBEW.STPRS
     Product Code   → VBRP.MATNR
     Branch ID      → VBRP.VKBUR
     Warehouse ID   → VBRP.LGORT
     Executive ID   → VBPA.PERNR
     Warehouse Name → T001L.LGOBE
     Executive Name → PA0001.ENAME

   Grain: ITEM LEVEL (VBELN + POSNR)
   ============================================================ */

WITH

/* ── Sold-to Customer Name ─────────────────────────────────── */
CTE_SOLDTO_NAME AS (
    SELECT
        kunnr,
        name1                   AS customer_name,
        name2                   AS customer_name2,
        ort01                   AS city,
        land1                   AS country
    FROM "sap_stg_ecc"."stg_sap_vf05n_v2_kna1"
),

/* ── Cancellation Status ───────────────────────────────────── */
CTE_CANCEL_STATUS AS (
    SELECT
        vbeln,
        CASE
            WHEN fksto = 'X' THEN 'Y'
            ELSE 'N'
        END                     AS is_cancelled,
        NULLIF(sfakn, '')       AS cancelled_by_doc
    FROM "sap_stg_ecc"."stg_sap_vf05n_v2_vbrk"
),

/* ── Executive Mapping ─────────────────────────────────────── */
CTE_EXECUTIVE AS (
    SELECT DISTINCT ON (vbeln)
        vbeln,
        pernr                   AS executive_id
    FROM "sap_stg_ecc"."stg_sap_vf05n_v2_vbpa"
    WHERE pernr <> ''
    ORDER BY
        vbeln,
        posnr ASC
),

/* ── Executive Name ────────────────────────────────────────── */
CTE_EXEC_NAME AS (
    SELECT
        pernr,
        ename                   AS executive_name,
        sname                   AS executive_name_sort
    FROM "sap_stg_ecc"."stg_sap_vf05n_v2_pa0001"
),

/* ── Warehouse Description ─────────────────────────────────── */
CTE_WAREHOUSE AS (
    SELECT
        werks,
        lgort,
        lgobe                   AS warehouse_name
    FROM "sap_stg_ecc"."stg_sap_vf05n_v2_t001l"
),

/* ── Material Cost from MBEW ───────────────────────────────── */
CTE_MBEW AS (
    SELECT
        matnr,
        bwkey,
        stprs                   AS standard_price
    FROM "sap_stg_ecc"."stg_sap_vf05n_v2_mbew"
)

/* ═════════════════════════════════════════════════════════════
   MAIN SELECT
   ═════════════════════════════════════════════════════════════ */
SELECT

    /* ── Billing Identification ───────────────────────────── */
    vb.vbeln                                        AS "Bill.Doc.",
    vp.posnr                                        AS "Item",

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

    /* ── Dates ────────────────────────────────────────────── */
    vb.fkdat                                        AS "Bill. Date",
    vb.erdat                                        AS "Created On",
    vb.erzet                                        AS "Created At",

    /* ── Organisation ─────────────────────────────────────── */
    vb.bukrs                                        AS "Company Code",
    vb.vkorg                                        AS "SOrg.",
    vb.vtweg                                        AS "DChl",
    vb.spart                                        AS "Division",

    /* ── Customer ─────────────────────────────────────────── */
    vb.kunag                                        AS "Sold-to pt",

    COALESCE(
        NULLIF(sn.customer_name, ''),
        '— customer not in master —'
    )                                               AS "Customer Name",

    sn.city                                         AS "City",
    sn.country                                      AS "Country",

    vb.kunrg                                        AS "Payer",

    /* ── Product / Item ───────────────────────────────────── */
    vp.matnr                                        AS "Product Code",
    vp.arktx                                        AS "Material Description",

    CAST(NULLIF(vp.fkimg, '') AS NUMERIC(18,3))     AS "Sale Quantity",

    vp.vrkme                                        AS "Sales Unit",

    /* ── Branch / Warehouse ───────────────────────────────── */
    vp.vkbur                                        AS "Branch ID",

    vp.lgort                                        AS "Warehouse ID",

    vp.werks                                        AS "Plant",

    COALESCE(
        NULLIF(wh.warehouse_name, ''),
        '— warehouse not in master —'
    )                                               AS "Warehouse Name",

    /* ── Executive ────────────────────────────────────────── */
    COALESCE(
        NULLIF(ex.executive_id, ''),
        '—'
    )                                               AS "Executive ID",

    COALESCE(
        NULLIF(en.executive_name, ''),
        '— executive not in master —'
    )                                               AS "Executive Name",

    /* ── Existing COGS from VBRP.WAVWR ────────────────────── */
    CASE
        WHEN vb.waerk IN ('UGX', 'RWF')
        THEN CAST(NULLIF(vp.wavwr, '') AS NUMERIC(18,2)) * 100
        ELSE CAST(NULLIF(vp.wavwr, '') AS NUMERIC(18,2))
    END                                             AS "COGS",

    /* ── New COGS from MBEW.STPRS ─────────────────────────── */
    CASE
        WHEN vb.waerk IN ('UGX', 'RWF')
        THEN (
            CAST(NULLIF(vp.fkimg, '') AS NUMERIC(18,3))
            * CAST(NULLIF(mb.standard_price, '') AS NUMERIC(18,2))
        ) * 100

        ELSE
            CAST(NULLIF(vp.fkimg, '') AS NUMERIC(18,3))
            * CAST(NULLIF(mb.standard_price, '') AS NUMERIC(18,2))
    END                                             AS "COGS_MBEW",

    /* ── Item Net Value ───────────────────────────────────── */
    CASE
        WHEN vb.waerk IN ('UGX', 'RWF')
        THEN CAST(NULLIF(vp.netwr, '') AS NUMERIC(18,2)) * 100
        ELSE CAST(NULLIF(vp.netwr, '') AS NUMERIC(18,2))
    END                                             AS "Item Net Value",

    /* ── Header Net Value ─────────────────────────────────── */
    CASE
        WHEN vb.waerk IN ('UGX', 'RWF')
        THEN CAST(NULLIF(vb.netwr, '') AS NUMERIC(18,2)) * 100
        ELSE CAST(NULLIF(vb.netwr, '') AS NUMERIC(18,2))
    END                                             AS "Doc Net Value",

    vb.waerk                                        AS "Curr.",

    /* ── Tax ──────────────────────────────────────────────── */
    CASE
        WHEN vb.waerk IN ('UGX', 'RWF')
        THEN CAST(NULLIF(vb.mwsbk, '') AS NUMERIC(18,2)) * 100
        ELSE CAST(NULLIF(vb.mwsbk, '') AS NUMERIC(18,2))
    END                                             AS "Tax Amount",

    /* ── Gross Value ──────────────────────────────────────── */
    CASE
        WHEN vb.waerk IN ('UGX', 'RWF')
        THEN (
            CAST(NULLIF(vb.netwr, '') AS NUMERIC(18,2))
            + CAST(NULLIF(vb.mwsbk, '') AS NUMERIC(18,2))
        ) * 100

        ELSE
            CAST(NULLIF(vb.netwr, '') AS NUMERIC(18,2))
            + CAST(NULLIF(vb.mwsbk, '') AS NUMERIC(18,2))
    END                                             AS "Gross Value",

    /* ── Posting Status ───────────────────────────────────── */
    vb.rfbsk                                        AS "PsSt",

    CASE vb.rfbsk
        WHEN 'A' THEN 'Not yet posted'
        WHEN 'B' THEN 'Partially transferred'
        WHEN 'C' THEN 'Fully posted'
        WHEN 'D' THEN 'Cleared in FI'
        ELSE 'Unknown'
    END                                             AS "Posting Status Text",

    /* ── Accounting ───────────────────────────────────────── */
    vb.belnr                                        AS "Accounting Doc.",
    vb.gjahr                                        AS "Fiscal Year",

    /* ── Cancellation ─────────────────────────────────────── */
    cs.is_cancelled                                 AS "Cancelled",
    cs.cancelled_by_doc                             AS "Cancellation Doc",

    /* ── Reference ────────────────────────────────────────── */
    vb.bstnk_vf                                     AS "Customer PO",
    vb.zterm                                        AS "Payment Terms",

    /* ── Audit ────────────────────────────────────────────── */
    vb.ernam                                        AS "Created"

FROM "sap_stg_ecc"."stg_sap_vf05n_v2_vbrk" vb

INNER JOIN "sap_stg_ecc"."stg_sap_vf05n_v2_vbrp" vp
    ON vp.vbeln = vb.vbeln

LEFT JOIN CTE_SOLDTO_NAME sn
    ON sn.kunnr = vb.kunag

LEFT JOIN CTE_CANCEL_STATUS cs
    ON cs.vbeln = vb.vbeln

LEFT JOIN CTE_EXECUTIVE ex
    ON ex.vbeln = vb.vbeln

LEFT JOIN CTE_EXEC_NAME en
    ON en.pernr = ex.executive_id

LEFT JOIN CTE_WAREHOUSE wh
    ON wh.werks = vp.werks
    AND wh.lgort = vp.lgort

LEFT JOIN CTE_MBEW mb
    ON mb.matnr = vp.matnr
    AND mb.bwkey = vp.werks

ORDER BY
    vb.fkdat DESC,
    vb.vbeln ASC,
    vp.posnr ASC;
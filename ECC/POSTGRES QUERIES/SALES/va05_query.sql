WITH CTE_DELIVERY AS (
    SELECT
        vbeln,
        posnr,
        etenr,
        edatu       AS delivery_date,
        wmeng,
        bmeng
    FROM "sap_stg_ecc"."stg_sap_va05_vbep"
    WHERE edatu IS NOT NULL
      AND edatu <> ''
      AND edatu <> '00000000'
),

CTE_SOLDTO AS (
    SELECT
        vbeln,
        kunnr   AS soldto_kunnr
    FROM "sap_stg_ecc"."stg_sap_va05_vbpa"
    WHERE parvw  = 'AG'
      AND posnr  = '000000'
),

-- NEW: Employee Responsible lookup
CTE_EMP_RESP AS (
    SELECT
        vbeln,
        posnr,
        pernr   AS emp_pernr
    FROM "sap_stg_ecc"."stg_sap_va05_vbpa_er"
    WHERE parvw = 'ZM'
	AND posnr = '000000'
),

CTE_RETURNS AS (
    SELECT DISTINCT
        vbelv   AS vbeln,
        posnv   AS posnr,
        'Y'     AS has_return
    FROM "sap_stg_ecc"."stg_sap_va05_vbfa"
    WHERE vbtyp_n = 'H'
),

CTE_INVOICE AS (
    SELECT DISTINCT
        vbelv   AS vbeln,
        posnv   AS posnr,
        'Y'     AS has_invoice
    FROM "sap_stg_ecc"."stg_sap_va05_vbfa"
    WHERE vbtyp_n = 'M'
),

CTE_DELIVERY_DOC AS (
    SELECT DISTINCT
        vbelv   AS vbeln,
        posnv   AS posnr,
        'Y'     AS has_delivery
    FROM "sap_stg_ecc"."stg_sap_va05_vbfa"
    WHERE vbtyp_n = 'J'
),

CTE_STORAGE_LOC AS (
    SELECT
        f.vbelv AS vbeln,
        f.posnv AS posnr,
        MAX(l.lgort) AS lgort
    FROM "sap_stg_ecc"."stg_sap_va05_vbfa" f
    INNER JOIN "sap_stg_ecc"."stg_sap_va05_lips" l 
        ON l.vbeln = f.vbeln AND l.posnr = f.posnn
    WHERE f.vbtyp_n = 'J'
    GROUP BY f.vbelv, f.posnv
)

SELECT
    /* ── Primary Organizational Data ──────────────────────── */
    ak.bukrs_vf                                 AS "Company Code",
    ap.werks                                    AS "Plant",
    sl.lgort                                    AS "Storage Location",

    /* ── Document Details ─────────────────────────────────── */
    ak.vbeln                                    AS "Document",
    ak.audat                                    AS "Doc. Date",
    ak.bstnk                                    AS "PO Number",
    ap.posnr                                    AS "Item",
    ep.etenr                                    AS "Sched. Line",
    ap.matnr                                    AS "Material",

    COALESCE(
        NULLIF(mk.maktx, ''),
        NULLIF(ap.arktx, ''),
        '— no description —'
    )                                           AS "Description",

    /* ── Quantities & Units ───────────────────────────────── */
    CAST(NULLIF(ap.kwmeng, '') AS NUMERIC(18,3)) AS "Total Item Qty",
    CAST(NULLIF(ep.wmeng, '')  AS NUMERIC(18,3)) AS "Sched. Line Qty",
    CAST(NULLIF(ep.bmeng, '')  AS NUMERIC(18,3)) AS "Confirmed Qty", 
    ap.vrkme                                    AS "Unit / SU",

    /* ── Scaled Net Value Logic ───────────────────────────── */
    CASE 
        WHEN ak.waerk IN ('UGX', 'RWF') 
        THEN CAST(NULLIF(ap.netwr, '') AS NUMERIC(18,2)) * 100
        ELSE CAST(NULLIF(ap.netwr, '') AS NUMERIC(18,2))
    END                                         AS "Net Value",
    
    ak.waerk                                    AS "Curr.",

    /* ── Customer Details ─────────────────────────────────── */
    COALESCE(
        NULLIF(pa.soldto_kunnr, ''),
        NULLIF(ak.kunnr, '')
    )                                           AS "Sold-to",
    COALESCE(
        NULLIF(kn.name1, ''),
        '— customer not in master —'
    )                                           AS "Name 1",

    /* ── Employee Responsible Details ─────────────────────── */
    er.emp_pernr                                AS "Personnel No.",
    COALESCE(NULLIF(hr.vorna, ''), '')          AS "First Name",
    COALESCE(NULLIF(hr.nachn, ''), '')          AS "Last Name",

    /* ── Statuses & Flags ─────────────────────────────────── */
    ep.delivery_date                            AS "Dlv. Date",
    uk.gbstk                                    AS "Overall Status",
    up.gbsta                                    AS "Item Status",

    CASE uk.gbstk
        WHEN 'A' THEN 'Not processed'
        WHEN 'B' THEN 'Partially processed'
        WHEN 'C' THEN 'Fully processed'
        ELSE          'Open'
    END                                         AS "Overall Status Text",

    CASE up.gbsta
        WHEN 'A' THEN 'Not processed'
        WHEN 'B' THEN 'Partially processed'
        WHEN 'C' THEN 'Fully processed'
        ELSE          'Open'
    END                                         AS "Item Status Text",

    COALESCE(ret.has_return,   'N')             AS "Has Return",
    COALESCE(inv.has_invoice,  'N')             AS "Has Invoice",
    COALESCE(dlv.has_delivery, 'N')             AS "Has Delivery",
    ak.vkgrp                                    AS "Sales Group"

FROM      "sap_stg_ecc"."stg_sap_va05_vbak" ak
INNER JOIN "sap_stg_ecc"."stg_sap_va05_vbap" ap ON ap.vbeln = ak.vbeln
INNER JOIN CTE_DELIVERY      ep  ON ep.vbeln = ap.vbeln AND ep.posnr = ap.posnr
LEFT JOIN CTE_SOLDTO         pa  ON pa.vbeln = ak.vbeln
LEFT JOIN "sap_stg_ecc"."stg_sap_va05_kna1" kn ON kn.kunnr = COALESCE(NULLIF(pa.soldto_kunnr,''), ak.kunnr)
LEFT JOIN CTE_EMP_RESP        er  ON er.vbeln = ak.vbeln
LEFT JOIN "sap_stg_ecc"."stg_sap_va05_pa0002" hr ON hr.pernr = er.emp_pernr
LEFT JOIN "sap_stg_ecc"."stg_sap_va05_vbuk" uk ON uk.vbeln = ak.vbeln
LEFT JOIN "sap_stg_ecc"."stg_sap_va05_vbup" up ON up.vbeln = ap.vbeln AND up.posnr = ap.posnr
LEFT JOIN CTE_RETURNS        ret ON ret.vbeln = ap.vbeln AND ret.posnr = ap.posnr
LEFT JOIN CTE_INVOICE        inv ON inv.vbeln = ap.vbeln AND inv.posnr = ap.posnr
LEFT JOIN CTE_DELIVERY_DOC   dlv ON dlv.vbeln = ap.vbeln AND dlv.posnr = ap.posnr
LEFT JOIN CTE_STORAGE_LOC    sl  ON sl.vbeln = ap.vbeln AND sl.posnr = ap.posnr
LEFT JOIN "sap_stg_ecc"."stg_sap_va05_makt" mk  ON mk.matnr = ap.matnr

ORDER BY
    ak.audat  DESC,
    ak.vbeln  ASC,
    ap.posnr  ASC,
    ep.etenr  ASC;
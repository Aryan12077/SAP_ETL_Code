/* ============================================================
    COOIS — Production Order Operations | PostgreSQL (ECC)    v2
    Source schema: sap_stg_ecc   (extractor COOIS v1.1.0 ECC,
                                  AUFK restricted to AUTYP '10')

    CHANGE LOG (search for ">>> CHANGED" to find each edit)
      v2-1. CTE_ORDER_TYPE_TEXT (new) : order type text from T003P
                                        (was a hardcoded CASE)
      v2-2. CTE_ORDER_STATUS  : DISTINCT (objnr, stat) before aggregation
      v2-3. CTE_PROFIT_CENTER : one row per profit centre (the join is on
                                PRCTR only, so the old PRCTR+KOKRS ranking
                                could duplicate rows); latest validity wins,
                                no CURRENT_DATE dependency
      v2-4. Old commented-out v1 removed from the file

    Output columns: unchanged (same names, same order, same types).
   ============================================================ */

WITH CTE_MATERIAL_DESC AS (
    SELECT
        matnr,
        maktx AS material_description
    FROM "sap_stg_ecc"."stg_sap_coois_makt"
    WHERE spras = 'E'
),

/* >>> CHANGED (v2-1): new CTE, replaces the hardcoded order type CASE */
CTE_ORDER_TYPE_TEXT AS (
    SELECT DISTINCT ON (auart)
        auart,
        NULLIF(TRIM(txt), '')   AS order_type_text
    FROM "sap_stg_ecc"."stg_sap_coois_t003p"
    WHERE spras = 'E'
    ORDER BY auart
),

CTE_ORDER_STATUS AS (
    SELECT
        je.objnr,

        STRING_AGG(
            COALESCE(NULLIF(tj.txt04, ''), je.stat),
            ' | '
            ORDER BY je.stat
        )                                           AS status_codes,

        STRING_AGG(
            COALESCE(NULLIF(tj.txt30, ''), je.stat),
            ' | '
            ORDER BY je.stat
        )                                           AS status_texts,

        MAX(CASE WHEN tj.txt04 = 'REL'  THEN 1 ELSE 0 END) AS is_released,
        MAX(CASE WHEN tj.txt04 = 'TECO' THEN 1 ELSE 0 END) AS is_teco,
        MAX(CASE WHEN tj.txt04 = 'CLSD' THEN 1 ELSE 0 END) AS is_closed,
        MAX(CASE WHEN tj.txt04 = 'DLV'  THEN 1 ELSE 0 END) AS is_delivered,
        MAX(CASE WHEN tj.txt04 = 'PDLV' THEN 1 ELSE 0 END) AS is_part_delivered,
        MAX(CASE WHEN tj.txt04 = 'CRTD' THEN 1 ELSE 0 END) AS is_created

    /* >>> CHANGED (v2-2): DISTINCT removes duplicate staging rows */
    FROM (
        SELECT DISTINCT objnr, stat
        FROM "sap_stg_ecc"."stg_sap_coois_jest"
        WHERE COALESCE(TRIM(inact), '') = ''
    ) je
    LEFT JOIN "sap_stg_ecc"."stg_sap_coois_tj02t" tj
        ON tj.istat = je.stat
        AND tj.spras = 'E'
    GROUP BY je.objnr
),

CTE_ORDER_PROGRESS AS (
    SELECT
        aufnr,
        posnr,
        CAST(NULLIF(psmng, '') AS NUMERIC(18,3))    AS qty_planned,
        CAST(NULLIF(wemng, '') AS NUMERIC(18,3))    AS qty_delivered,

        CASE
            WHEN CAST(NULLIF(psmng, '') AS NUMERIC(18,3)) IS NULL THEN NULL
            WHEN CAST(NULLIF(wemng, '') AS NUMERIC(18,3)) IS NULL
                THEN CAST(NULLIF(psmng, '') AS NUMERIC(18,3))
            WHEN CAST(NULLIF(wemng, '') AS NUMERIC(18,3))
                 >= CAST(NULLIF(psmng, '') AS NUMERIC(18,3)) THEN 0
            ELSE CAST(NULLIF(psmng, '') AS NUMERIC(18,3))
               - CAST(NULLIF(wemng, '') AS NUMERIC(18,3))
        END                                         AS qty_open,

        CASE
            WHEN CAST(NULLIF(psmng, '') AS NUMERIC(18,3)) IS NULL
              OR CAST(NULLIF(psmng, '') AS NUMERIC(18,3)) = 0 THEN NULL
            ELSE ROUND(
                100.0 * CAST(NULLIF(wemng, '') AS NUMERIC(18,3))
                / CAST(NULLIF(psmng, '') AS NUMERIC(18,3)),
                2
            )
        END                                         AS pct_delivered
    FROM "sap_stg_ecc"."stg_sap_coois_afpo"
),

CTE_PROFIT_CENTER AS (
    SELECT
        prctr,
        kokrs,
        ktext   AS profit_center_name,
        ltext   AS profit_center_long_text
    FROM (
        SELECT
            prctr,
            kokrs,
            ktext,
            ltext,
            /* >>> CHANGED (v2-3): one row per PRCTR (the join key); latest
                   validity first. If ECC has several controlling areas, add
                   AND kokrs = '<your KOKRS>' to the WHERE below. */
            ROW_NUMBER() OVER (
                PARTITION BY prctr
                ORDER BY
                    COALESCE(NULLIF(TRIM(datbi), ''), '99991231') DESC,
                    kokrs
            ) AS rn
        FROM "sap_stg_ecc"."stg_sap_coois_cepct"
        WHERE spras = 'E'
    ) sub
    WHERE rn = 1
)

/* ──────────────────────────────────────────────────────────────
    MAIN SELECT
   ────────────────────────────────────────────────────────────── */
SELECT

    ak.aufnr                                        AS "Order",
    ak.auart                                        AS "Order Type",

    /* >>> CHANGED (v2-1): text from T003P, falls back to the order type */
    COALESCE(ot.order_type_text, ak.auart)          AS "Order Type Text",

    ak.ktext                                        AS "Order Description",

    ak.werks                                        AS "Plant",
    ak.prctr                                        AS "Profit Center",
    pc.profit_center_name                           AS "Profit Center Name",
    pc.profit_center_long_text                      AS "Profit Center Long Text",
    ak.kostl                                        AS "Cost Center",

    ak.erdat                                        AS "Created on",

    ko.gstrp                                        AS "Bsc start",
    ko.gltrp                                        AS "Basic fin.",
    ko.gstrs                                        AS "Sched. Start",
    ko.gltrs                                        AS "Sched. Finish",
    ko.gstri                                        AS "Actual Start",
    ko.gltri                                        AS "Actual Finish",

    ko.fevor                                        AS "Prod. Supervisor",
    ko.plgrp                                        AS "Planner Group",

    po.posnr                                        AS "Item",
    po.matnr                                        AS "Material",

    COALESCE(NULLIF(md.material_description, ''), '— description not found —')
                                                    AS "Material Description",

    po.dwerk                                        AS "Production Plant",
    po.lgort                                        AS "Storage Location",
    po.verid                                        AS "Prod. Version",

    pr.qty_planned                                  AS "Target qty",
    pr.qty_delivered                                AS "Del. qty",
    pr.qty_open                                     AS "Open qty",
    pr.pct_delivered                                AS "% Delivered",

    po.meins                                        AS "Unit",

    CAST(NULLIF(ko.gamng, '') AS NUMERIC(18,3))     AS "Total Order Qty",
    ko.gmein                                        AS "Header UoM",

    po.elikz                                        AS "Del. Completed",
    CASE po.elikz WHEN 'X' THEN 'Yes' ELSE 'No' END AS "Del. Completed Text",

    po.ltrmp                                        AS "Planned Del. Date",
    po.ltrmi                                        AS "Actual Del. Date",

    os.status_codes                                 AS "System Status",
    os.status_texts                                 AS "System Status Text",

    CASE os.is_released WHEN 1 THEN 'Y' ELSE 'N' END        AS "Released",
    CASE os.is_teco WHEN 1 THEN 'Y' ELSE 'N' END            AS "Tech. Completed",
    CASE os.is_closed WHEN 1 THEN 'Y' ELSE 'N' END          AS "Closed",
    CASE os.is_delivered WHEN 1 THEN 'Y' ELSE 'N' END       AS "Fully Delivered",
    CASE os.is_part_delivered WHEN 1 THEN 'Y' ELSE 'N' END  AS "Part. Delivered",

    ak.loekz                                        AS "Del. Flag",
    CASE ak.loekz WHEN 'X' THEN 'Y' ELSE 'N' END   AS "Deletion Flagged"

FROM "sap_stg_ecc"."stg_sap_coois_aufk" ak

LEFT JOIN "sap_stg_ecc"."stg_sap_coois_afko" ko
    ON ko.aufnr = ak.aufnr

LEFT JOIN "sap_stg_ecc"."stg_sap_coois_afpo" po
    ON po.aufnr = ak.aufnr

LEFT JOIN CTE_MATERIAL_DESC md
    ON md.matnr = po.matnr

LEFT JOIN CTE_ORDER_PROGRESS pr
    ON pr.aufnr = po.aufnr
    AND pr.posnr = po.posnr

LEFT JOIN CTE_ORDER_STATUS os
    ON os.objnr = ak.objnr

LEFT JOIN CTE_PROFIT_CENTER pc
    ON pc.prctr = ak.prctr

LEFT JOIN CTE_ORDER_TYPE_TEXT ot
    ON ot.auart = ak.auart

ORDER BY
    ak.erdat DESC,
    ak.aufnr ASC,
    po.posnr ASC;
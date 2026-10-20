/* ============================================================
    COOIS — Production Order Operations | PostgreSQL
   ============================================================ */

WITH CTE_MATERIAL_DESC AS (
    SELECT
        matnr,
        maktx AS material_description
    FROM "sap_stg_hana"."stg_sap_coois_makt"
    WHERE spras = 'E'
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

    FROM "sap_stg_hana"."stg_sap_coois_jest" je
    LEFT JOIN "sap_stg_hana"."stg_sap_coois_tj02t" tj
        ON tj.istat = je.stat
        AND tj.spras = 'E'
    WHERE je.inact = ''
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
    FROM "sap_stg_hana"."stg_sap_coois_afpo"
)

/* ──────────────────────────────────────────────────────────────
    MAIN SELECT
   ────────────────────────────────────────────────────────────── */
SELECT

    ak.aufnr                                        AS "Order",
    ak.auart                                        AS "Order Type",

    CASE ak.auart
        WHEN 'PP01' THEN 'Standard Production'
        WHEN 'PP02' THEN 'Repetitive Manufacturing'
        WHEN 'PP03' THEN 'Campaign Order'
        WHEN 'PM01' THEN 'Maintenance Order'
        WHEN 'SM01' THEN 'Service Order'
        ELSE ak.auart
    END                                             AS "Order Type Text",

    ak.ktext                                        AS "Order Description",

    ak.werks                                        AS "Plant",
    ak.prctr                                        AS "Profit Center",
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

FROM "sap_stg_hana"."stg_sap_coois_aufk" ak

LEFT JOIN "sap_stg_hana"."stg_sap_coois_afko" ko
    ON ko.aufnr = ak.aufnr

LEFT JOIN "sap_stg_hana"."stg_sap_coois_afpo" po
    ON po.aufnr = ak.aufnr

LEFT JOIN CTE_MATERIAL_DESC md
    ON md.matnr = po.matnr

LEFT JOIN CTE_ORDER_PROGRESS pr
    ON pr.aufnr = po.aufnr
    AND pr.posnr = po.posnr

LEFT JOIN CTE_ORDER_STATUS os
    ON os.objnr = ak.objnr

ORDER BY
    ak.erdat DESC,
    ak.aufnr ASC,
    po.posnr ASC;

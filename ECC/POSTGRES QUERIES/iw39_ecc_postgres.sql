/* ============================================================
    IW39 — Display PM Orders | PostgreSQL
   ============================================================ */

WITH CTE_EQUIPMENT_DESC AS (
    SELECT
        equnr,
        eqktx                   AS equipment_desc
    FROM "sap_stg_ecc"."stg_sap_iw39_eqkt"
    WHERE spras = 'E'
),

CTE_FUNC_LOC AS (
    SELECT
        il.iloan,
        il.tplnr                AS functional_loc,
        il.swerk                 AS plant_iloa,
        il.kostl                AS cost_center_iloa,
        fx.pltxt                AS func_loc_desc
    FROM "sap_stg_ecc"."stg_sap_iw39_iloa"      il
    LEFT JOIN "sap_stg_ecc"."stg_sap_iw39_iflotx" fx
        ON  fx.tplnr = il.tplnr
        AND fx.spras = 'E'
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

        MAX(CASE WHEN tj.txt04 = 'CRTD' THEN 1 ELSE 0 END)  AS is_created,
        MAX(CASE WHEN tj.txt04 = 'REL'  THEN 1 ELSE 0 END)  AS is_released,
        MAX(CASE WHEN tj.txt04 = 'TECO' THEN 1 ELSE 0 END)  AS is_teco,
        MAX(CASE WHEN tj.txt04 = 'CLSD' THEN 1 ELSE 0 END)  AS is_closed,
        MAX(CASE WHEN tj.txt04 = 'PRC'  THEN 1 ELSE 0 END)  AS is_part_confirmed,
        MAX(CASE WHEN tj.txt04 = 'CNF'  THEN 1 ELSE 0 END)  AS is_confirmed,
        MAX(CASE WHEN tj.txt04 = 'GMPS' THEN 1 ELSE 0 END)  AS is_goods_posted,
        MAX(CASE WHEN tj.txt04 = 'SETC' THEN 1 ELSE 0 END)  AS is_settled

    FROM "sap_stg_ecc"."stg_sap_iw39_jest"          je
    LEFT JOIN "sap_stg_ecc"."stg_sap_iw39_tj02t"    tj
        ON  tj.istat = je.stat
        AND tj.spras = 'E'
    WHERE je.inact = ''
    GROUP BY je.objnr
),

CTE_ACTUAL_COSTS AS (
    SELECT
        objnr,

        SUM(
            COALESCE(CAST(NULLIF(wrt01, '') AS NUMERIC(18,2)), 0)
          + COALESCE(CAST(NULLIF(wrt02, '') AS NUMERIC(18,2)), 0)
          + COALESCE(CAST(NULLIF(wrt03, '') AS NUMERIC(18,2)), 0)
          + COALESCE(CAST(NULLIF(wrt04, '') AS NUMERIC(18,2)), 0)
          + COALESCE(CAST(NULLIF(wrt05, '') AS NUMERIC(18,2)), 0)
          + COALESCE(CAST(NULLIF(wrt06, '') AS NUMERIC(18,2)), 0)
          + COALESCE(CAST(NULLIF(wrt07, '') AS NUMERIC(18,2)), 0)
          + COALESCE(CAST(NULLIF(wrt08, '') AS NUMERIC(18,2)), 0)
          + COALESCE(CAST(NULLIF(wrt09, '') AS NUMERIC(18,2)), 0)
          + COALESCE(CAST(NULLIF(wrt10, '') AS NUMERIC(18,2)), 0)
          + COALESCE(CAST(NULLIF(wrt11, '') AS NUMERIC(18,2)), 0)
          + COALESCE(CAST(NULLIF(wrt12, '') AS NUMERIC(18,2)), 0)
          + COALESCE(CAST(NULLIF(wrt13, '') AS NUMERIC(18,2)), 0)
          + COALESCE(CAST(NULLIF(wrt14, '') AS NUMERIC(18,2)), 0)
          + COALESCE(CAST(NULLIF(wrt15, '') AS NUMERIC(18,2)), 0)
          + COALESCE(CAST(NULLIF(wrt16, '') AS NUMERIC(18,2)), 0)
        )                       AS total_actual_costs,

        MAX(NULLIF(cocur, ''))  AS cost_currency

    FROM "sap_stg_ecc"."stg_sap_iw39_pmco"
    WHERE wrttp = '04'
    GROUP BY objnr
)

/* ──────────────────────────────────────────────────────────────
    MAIN SELECT
   ────────────────────────────────────────────────────────────── */
SELECT

    ak.aufnr                                        AS "Order",
    ak.auart                                        AS "Order Type",

    CASE ak.auart
        WHEN 'PM01' THEN 'Corrective Maintenance'
        WHEN 'PM02' THEN 'Breakdown Maintenance'
        WHEN 'PM03' THEN 'Preventive Maintenance'
        WHEN 'PM04' THEN 'Refurbishment Order'
        WHEN 'ZPM3' THEN 'Preventive Maintenance'
        ELSE ak.auart
    END                                             AS "Order Type Text",

    ak.autyp                                        AS "Order Category",
    CASE ak.autyp
        WHEN '10' THEN 'Internal Order'
        WHEN '20' THEN 'Investment Order'
        WHEN '30' THEN 'PM Order'
        WHEN '40' THEN 'Process Order'
        ELSE ak.autyp
    END                                             AS "Order Category Text",

    ak.ktext                                        AS "Description",

    ak.werks                                        AS "Plant",
    ak.bukrs                                        AS "Company Code",
    ak.prctr                                        AS "Profit Center",
    ak.kostl                                        AS "Cost Center",
    ak.waers                                        AS "Currency",

    ak.ernam                                        AS "Entered by",
    ak.erdat                                        AS "Created on",
    ak.aenam                                        AS "Changed by",
    ak.aedat                                        AS "Last Change Date",

    ai.equnr                                        AS "Equipment",
    COALESCE(
        NULLIF(ed.equipment_desc, ''),
        '— description not found —'
    )                                               AS "Equipment Description",
    ai.iwerk                                        AS "Maint. Planning Plant",
    ai.ingpr                                        AS "Planner group",
    ai.warpl                                        AS "MaintenancePlan",
    ai.priok                                        AS "Priority",
    ai.ilart                                        AS "Maint. Activity Type",
    ai.qmnum                                        AS "Notification",
    ai.anlzu                                        AS "System Condition",
    ai.iphas                                        AS "Processing Phase",

    fl.functional_loc                               AS "Functional Loc.",
    COALESCE(
        NULLIF(fl.func_loc_desc, ''),
        '— func. loc. not found —'
    )                                               AS "Func. Loc. Description",
    fl.cost_center_iloa                             AS "Cost Center (Location)",

    ko.gstrp                                        AS "Bas. start date",
    ko.gltrp                                        AS "Basic fin. date",
    ko.gstrs                                        AS "Sched. start",
    ko.gltrs                                        AS "Sched. finish",
    ko.gstri                                        AS "Actual start",
    ko.gltri                                        AS "Actual finish",
    ko.ftrmi                                        AS "Actual release date",

    os.status_codes                                 AS "System status",
    os.status_texts                                 AS "System Status Text",

    CASE os.is_created       WHEN 1 THEN 'Y' ELSE 'N' END  AS "Created",
    CASE os.is_released      WHEN 1 THEN 'Y' ELSE 'N' END  AS "Released",
    CASE os.is_teco          WHEN 1 THEN 'Y' ELSE 'N' END  AS "Tech. Completed",
    CASE os.is_closed        WHEN 1 THEN 'Y' ELSE 'N' END  AS "Closed",
    CASE os.is_part_confirmed WHEN 1 THEN 'Y' ELSE 'N' END AS "Part. Confirmed",
    CASE os.is_confirmed     WHEN 1 THEN 'Y' ELSE 'N' END  AS "Confirmed",
    CASE os.is_goods_posted  WHEN 1 THEN 'Y' ELSE 'N' END  AS "Goods Posted",
    CASE os.is_settled       WHEN 1 THEN 'Y' ELSE 'N' END  AS "Settled",

    COALESCE(ac.total_actual_costs, 0.00)           AS "Total act.costs",
    ac.cost_currency                                AS "Cost Currency"

FROM      "sap_stg_ecc"."stg_sap_iw39_aufk"         ak

LEFT JOIN "sap_stg_ecc"."stg_sap_iw39_afih"         ai
    ON  ai.aufnr = ak.aufnr

LEFT JOIN "sap_stg_ecc"."stg_sap_iw39_afko"         ko
    ON  ko.aufnr = ak.aufnr

LEFT JOIN CTE_EQUIPMENT_DESC                 ed
    ON  ed.equnr = ai.equnr

LEFT JOIN CTE_FUNC_LOC                       fl
    ON  fl.iloan = ai.iloan

LEFT JOIN CTE_ORDER_STATUS                   os
    ON  os.objnr = ak.objnr

LEFT JOIN CTE_ACTUAL_COSTS                   ac
    ON  ac.objnr = ak.objnr

ORDER BY
    ak.erdat   DESC,
    ak.auart   ASC,
    ak.aufnr   ASC;

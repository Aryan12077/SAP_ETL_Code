/* ============================================================
    IW38 — List of PM Orders | PostgreSQL
   ============================================================ */

WITH CTE_EQUIP_DESC AS (
    SELECT
        equnr,
        eqktx                   AS equip_description
    FROM "sap_stg_hana"."stg_sap_iw38_eqkt"
    WHERE spras = 'E'
),

CTE_FUNCLOC_DESC AS (
    SELECT
        tplnr,
        pltxt                   AS funcloc_description
    FROM "sap_stg_hana"."stg_sap_iw38_iflotx"
    WHERE spras = 'E'
),

CTE_STATUS_RAW AS (
    SELECT
        j.objnr,
        t.txt04                 AS status_code
    FROM "sap_stg_hana"."stg_sap_iw38_jest"  j
    JOIN "sap_stg_hana"."stg_sap_iw38_tj02t" t
        ON  t.istat = j.stat
        AND t.spras = 'E'
    WHERE j.inact = ''
      AND NULLIF(t.txt04, '') IS NOT NULL
),

CTE_STATUS_CONCAT AS (
    SELECT
        objnr,
        STRING_AGG(status_code, ' ' ORDER BY status_code)
                                AS system_status
    FROM CTE_STATUS_RAW
    GROUP BY objnr
),

CTE_ACTUAL_COSTS AS (
    SELECT
        objnr,
        cocur                   AS cost_currency,
        SUM(
            COALESCE(CAST(NULLIF(wrt01, '') AS NUMERIC(18,2)), 0) +
            COALESCE(CAST(NULLIF(wrt02, '') AS NUMERIC(18,2)), 0) +
            COALESCE(CAST(NULLIF(wrt03, '') AS NUMERIC(18,2)), 0) +
            COALESCE(CAST(NULLIF(wrt04, '') AS NUMERIC(18,2)), 0) +
            COALESCE(CAST(NULLIF(wrt05, '') AS NUMERIC(18,2)), 0) +
            COALESCE(CAST(NULLIF(wrt06, '') AS NUMERIC(18,2)), 0) +
            COALESCE(CAST(NULLIF(wrt07, '') AS NUMERIC(18,2)), 0) +
            COALESCE(CAST(NULLIF(wrt08, '') AS NUMERIC(18,2)), 0) +
            COALESCE(CAST(NULLIF(wrt09, '') AS NUMERIC(18,2)), 0) +
            COALESCE(CAST(NULLIF(wrt10, '') AS NUMERIC(18,2)), 0) +
            COALESCE(CAST(NULLIF(wrt11, '') AS NUMERIC(18,2)), 0) +
            COALESCE(CAST(NULLIF(wrt12, '') AS NUMERIC(18,2)), 0) +
            COALESCE(CAST(NULLIF(wrt13, '') AS NUMERIC(18,2)), 0) +
            COALESCE(CAST(NULLIF(wrt14, '') AS NUMERIC(18,2)), 0) +
            COALESCE(CAST(NULLIF(wrt15, '') AS NUMERIC(18,2)), 0) +
            COALESCE(CAST(NULLIF(wrt16, '') AS NUMERIC(18,2)), 0)
        )                       AS total_actual_cost
    FROM "sap_stg_hana"."stg_sap_iw38_pmco"
    WHERE wrttp = '04'
    GROUP BY objnr, cocur
)

/* ──────────────────────────────────────────────────────────────
    MAIN SELECT
   ────────────────────────────────────────────────────────────── */
SELECT

    ak.gstrp                                        AS "Bas. start date",
    au.auart                                        AS "Order Type",

    CASE au.auart
        WHEN 'PM01' THEN 'Corrective Maintenance'
        WHEN 'PM02' THEN 'Breakdown Maintenance'
        WHEN 'ZPM3' THEN 'Preventive Maintenance'
        ELSE au.auart
    END                                             AS "Order Type Text",

    au.aufnr                                        AS "Order",
    ai.equnr                                        AS "Equipment",
    au.ktext                                        AS "Description",

    COALESCE(
        NULLIF(eq.equip_description, ''),
        '— equipment not in master —'
    )                                               AS "Equip. Description",

    il.tplnr                                        AS "Functional Loc.",

    COALESCE(
        NULLIF(fl.funcloc_description, ''),
        '— func. loc. not in master —'
    )                                               AS "Func. Loc. Description",

    NULLIF(ai.warpl, '')                            AS "MaintenancePlan",
    au.ernam                                        AS "Entered by",
    ai.iwerk                                        AS "Plant",
    ai.ingpr                                        AS "Planner group",

    COALESCE(pc.total_actual_cost, 0)               AS "Total act.costs",
    pc.cost_currency                                AS "Cost Currency",

    COALESCE(sc.system_status, '')                  AS "System status",

    au.aenam                                        AS "Changed by",
    au.erdat                                        AS "Created on",

    /* ── Additional fields ──────────────────────────────────── */
    au.autyp                                        AS "Order Category",
    au.bukrs                                        AS "Company Code",
    au.waers                                        AS "Order Currency",
    au.kostl                                        AS "Settlement Cost Center",
    au.prctr                                        AS "Profit Center",
    au.aedat                                        AS "Last Changed On",

    ai.priok                                        AS "Priority",
    ai.ilart                                        AS "Maint. Activity Type",
    ai.qmnum                                        AS "Notification No.",
    ai.anlzu                                        AS "System Condition",
    ai.iphas                                        AS "Processing Phase",

    ak.gltrp                                        AS "Basic Finish Date",
    ak.gstrs                                        AS "Scheduled Start",
    ak.gltrs                                        AS "Scheduled Finish",
    ak.gstri                                        AS "Actual Start",
    ak.gltri                                        AS "Actual Finish",
    ak.ftrmi                                        AS "Actual Release Date"

FROM      "sap_stg_hana"."stg_sap_iw38_aufk"         au

INNER JOIN "sap_stg_hana"."stg_sap_iw38_afih"        ai
    ON  ai.aufnr = au.aufnr

LEFT  JOIN "sap_stg_hana"."stg_sap_iw38_afko"        ak
    ON  ak.aufnr = au.aufnr

LEFT  JOIN CTE_EQUIP_DESC              eq
    ON  eq.equnr = ai.equnr

LEFT  JOIN "sap_stg_hana"."stg_sap_iw38_iloa"        il
    ON  il.iloan = ai.iloan

LEFT  JOIN CTE_FUNCLOC_DESC            fl
    ON  fl.tplnr = il.tplnr

LEFT  JOIN CTE_STATUS_CONCAT           sc
    ON  sc.objnr = au.objnr

LEFT  JOIN CTE_ACTUAL_COSTS            pc
    ON  pc.objnr = au.objnr

ORDER BY
    ak.gstrp    ASC,
    au.aufnr    ASC;

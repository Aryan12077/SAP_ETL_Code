/* ============================================================
    IH08 — Equipment Register | PostgreSQL (Original)
   ============================================================ */
/*
WITH CTE_EQUIPMENT_DESC AS (
    SELECT
        equnr,
        eqktx                   AS equipment_name
    FROM "sap_stg_hana"."stg_sap_eqpreg_ih08_eqkt"
    WHERE spras = 'E'
),

CTE_LOCATION AS (
    SELECT
        ez.equnr,
        ez.iwerk                AS maint_plant_equz,
        il.swerk                AS maint_plant_iloa,
        il.stort                AS location,
        il.tplnr                AS functional_location,
        il.kostl                AS cost_center,
        il.iloan
    FROM "sap_stg_hana"."stg_sap_eqpreg_ih08_equz"   ez
    LEFT JOIN "sap_stg_hana"."stg_sap_eqpreg_ih08_iloa" il
        ON  il.iloan = ez.iloan
    WHERE ez.datbi = '99991231'
),

CTE_EQUIPMENT_STATUS AS (
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

        MAX(CASE WHEN tj.txt04 = 'AVLB' THEN 1 ELSE 0 END)  AS is_available,
        MAX(CASE WHEN tj.txt04 = 'INAC' THEN 1 ELSE 0 END)  AS is_inactive,
        MAX(CASE WHEN tj.txt04 = 'INST' THEN 1 ELSE 0 END)  AS is_installed,
        MAX(CASE WHEN tj.txt04 = 'DISM' THEN 1 ELSE 0 END)  AS is_dismantled,
        MAX(CASE WHEN tj.txt04 = 'ESTO' THEN 1 ELSE 0 END)  AS is_in_stock,
        MAX(CASE WHEN tj.txt04 = 'DLFL' THEN 1 ELSE 0 END)  AS is_deletion_flagged

    FROM "sap_stg_hana"."stg_sap_eqpreg_ih08_jest"          je
    LEFT JOIN "sap_stg_hana"."stg_sap_eqpreg_ih08_tj02t"    tj
        ON  tj.istat = je.stat
        AND tj.spras = 'E'
    WHERE je.inact = ''
    GROUP BY je.objnr
)

SELECT

    eq.equnr                                        AS "SAP Equipment No",
    COALESCE(
        NULLIF(ed.equipment_name, ''),
        '— description not found —'
    )                                               AS "Equipment Name",

    eq.eqtyp                                        AS "EquipCategory",
    CASE eq.eqtyp
        WHEN 'M' THEN 'Machines'
        WHEN 'E' THEN 'Electrical Equipment'
        WHEN 'I' THEN 'Instruments'
        WHEN 'P' THEN 'Pipelines'
        WHEN 'V' THEN 'Vehicles'
        ELSE eq.eqtyp
    END                                             AS "Equipment Category Text",

    lc.maint_plant_iloa                             AS "Plant",
    lc.maint_plant_equz                             AS "Maint. Plant (EQUZ)",
    eq.werk                                         AS "Equip. Plant (EQUI)",
    lc.location                                     AS "Location",
    lc.functional_location                          AS "Functional Location",
    lc.cost_center                                  AS "Cost Center",

    eq.herst                                        AS "Manufacturer",
    eq.herld                                        AS "Country of Origin",
    eq.typbz                                        AS "Model",
    eq.groes                                        AS "Capacity",
    eq.brgew                                        AS "Weight",
    eq.baujj                                        AS "Construction Year",

    es.status_codes                                 AS "System Status",
    es.status_texts                                 AS "Condition",

    CASE es.is_available        WHEN 1 THEN 'Y' ELSE 'N' END  AS "Available",
    CASE es.is_inactive         WHEN 1 THEN 'Y' ELSE 'N' END  AS "Inactive",
    CASE es.is_installed        WHEN 1 THEN 'Y' ELSE 'N' END  AS "Installed",
    CASE es.is_dismantled       WHEN 1 THEN 'Y' ELSE 'N' END  AS "Dismantled",
    CASE es.is_in_stock         WHEN 1 THEN 'Y' ELSE 'N' END  AS "In Stock",
    CASE es.is_deletion_flagged WHEN 1 THEN 'Y' ELSE 'N' END  AS "Deletion Flagged"

FROM      "sap_stg_hana"."stg_sap_eqpreg_ih08_equi"          eq

LEFT JOIN CTE_EQUIPMENT_DESC                          ed
    ON  ed.equnr = eq.equnr

LEFT JOIN CTE_LOCATION                                lc
    ON  lc.equnr = eq.equnr

LEFT JOIN CTE_EQUIPMENT_STATUS                        es
    ON  es.objnr = eq.objnr

ORDER BY
    eq.werk    ASC,
    eq.eqtyp   ASC,
    eq.equnr   ASC;
*/

/* ============================================================
    IH08 — Equipment Register | PostgreSQL
    (Modified with S070 MTTR/MTBF Aggregation and IFLOTX)
   ============================================================ */

WITH CTE_EQUIPMENT_DESC AS (
    SELECT
        equnr,
        eqktx                   AS equipment_name
    FROM "sap_stg_hana"."stg_sap_eqpreg_ih08_eqkt"
    WHERE spras = 'E'
),

CTE_LOCATION AS (
    SELECT
        ez.equnr,
        ez.iwerk                AS maint_plant_equz,
        il.swerk                AS maint_plant_iloa,
        il.stort                AS location,
        il.tplnr                AS functional_location,
        il.kostl                AS cost_center,
        il.iloan
    FROM "sap_stg_hana"."stg_sap_eqpreg_ih08_equz"   ez
    LEFT JOIN "sap_stg_hana"."stg_sap_eqpreg_ih08_iloa" il
        ON  il.iloan = ez.iloan
    WHERE ez.datbi = '99991231'
),

CTE_EQUIPMENT_STATUS AS (
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

        MAX(CASE WHEN tj.txt04 = 'AVLB' THEN 1 ELSE 0 END)  AS is_available,
        MAX(CASE WHEN tj.txt04 = 'INAC' THEN 1 ELSE 0 END)  AS is_inactive,
        MAX(CASE WHEN tj.txt04 = 'INST' THEN 1 ELSE 0 END)  AS is_installed,
        MAX(CASE WHEN tj.txt04 = 'DISM' THEN 1 ELSE 0 END)  AS is_dismantled,
        MAX(CASE WHEN tj.txt04 = 'ESTO' THEN 1 ELSE 0 END)  AS is_in_stock,
        MAX(CASE WHEN tj.txt04 = 'DLFL' THEN 1 ELSE 0 END)  AS is_deletion_flagged

    FROM "sap_stg_hana"."stg_sap_eqpreg_ih08_jest"          je
    LEFT JOIN "sap_stg_hana"."stg_sap_eqpreg_ih08_tj02t"    tj
        ON  tj.istat = je.stat
        AND tj.spras = 'E'
    WHERE je.inact = ''
    GROUP BY je.objnr
),

CTE_FUNCLOC_DESC AS (
    SELECT
        tplnr,
        pltxt AS funcloc_desc
    FROM "sap_stg_hana"."stg_sap_eqpreg_ih08_iflotx"
    WHERE spras = 'E'
),

CTE_S070_AGG AS (
    SELECT 
        equnr,
        -- Aggregate breakdown counts
        SUM(CAST(NULLIF(TRIM(smsaus), '') AS NUMERIC)) AS period_breakdowns,
        -- Aggregate downtime hours
        SUM(CAST(NULLIF(TRIM(sauszt), '') AS NUMERIC)) AS downtime_h,
        -- Aggregate time between repairs
        SUM(CAST(NULLIF(TRIM(seqtbr), '') AS NUMERIC)) AS time_between_repairs_h
    FROM "sap_stg_hana"."stg_sap_eqpreg_ih08_s070"
    GROUP BY equnr
)

/* ──────────────────────────────────────────────────────────────
    MAIN SELECT
   ────────────────────────────────────────────────────────────── */
SELECT

    eq.equnr                                        AS "SAP Equipment No",
    COALESCE(
        NULLIF(ed.equipment_name, ''),
        '— description not found —'
    )                                               AS "Equipment Name",

    eq.eqtyp                                        AS "EquipCategory",
    CASE eq.eqtyp
        WHEN 'M' THEN 'Machines'
        WHEN 'E' THEN 'Electrical Equipment'
        WHEN 'I' THEN 'Instruments'
        WHEN 'P' THEN 'Pipelines'
        WHEN 'V' THEN 'Vehicles'
		WHEN 'Q' THEN 'Test/Measurement Equipment'
		WHEN 'S' THEN 'Customer Equipment'
        ELSE eq.eqtyp
    END                                             AS "Equipment Category Text",

    lc.maint_plant_iloa                             AS "Plant",
    lc.maint_plant_equz                             AS "Maint. Plant (EQUZ)",
    eq.werk                                         AS "Equip. Plant (EQUI)",
    lc.location                                     AS "Location",
    lc.functional_location                          AS "Functional Location",
    fd.funcloc_desc                                 AS "Functional Location Desc",
    lc.cost_center                                  AS "Cost Center",

    eq.herst                                        AS "Manufacturer",
    eq.herld                                        AS "Country of Origin",
    eq.typbz                                        AS "Model",
    eq.groes                                        AS "Capacity",
    eq.brgew                                        AS "Weight",
    eq.baujj                                        AS "Construction Year",

    es.status_codes                                 AS "System Status",
    es.status_texts                                 AS "Condition",

    CASE es.is_available        WHEN 1 THEN 'Y' ELSE 'N' END  AS "Available",
    CASE es.is_inactive         WHEN 1 THEN 'Y' ELSE 'N' END  AS "Inactive",
    CASE es.is_installed        WHEN 1 THEN 'Y' ELSE 'N' END  AS "Installed",
    CASE es.is_dismantled       WHEN 1 THEN 'Y' ELSE 'N' END  AS "Dismantled",
    CASE es.is_in_stock         WHEN 1 THEN 'Y' ELSE 'N' END  AS "In Stock",
    CASE es.is_deletion_flagged WHEN 1 THEN 'Y' ELSE 'N' END  AS "Deletion Flagged",
    
    -- S070 Metrics
    COALESCE(s0.period_breakdowns, 0)               AS "Period Breakdowns",
    COALESCE(s0.downtime_h, 0)                      AS "Downtime (H)",
    COALESCE(s0.time_between_repairs_h, 0)          AS "Time Between Repairs (H)",
    
    -- MTTR = Total Downtime / Number of Breakdowns
    CASE 
        WHEN s0.period_breakdowns > 0 THEN ROUND((s0.downtime_h / s0.period_breakdowns), 2)
        ELSE 0 
    END                                             AS "MTTR (H)",
    
    -- MTBR = Total Time Between Repairs / Number of Breakdowns
    CASE 
        WHEN s0.period_breakdowns > 0 THEN ROUND((s0.time_between_repairs_h / s0.period_breakdowns), 2)
        ELSE 0 
    END                                             AS "MTBR (H)"

FROM      "sap_stg_hana"."stg_sap_eqpreg_ih08_equi"          eq

LEFT JOIN CTE_EQUIPMENT_DESC                          ed
    ON  ed.equnr = eq.equnr

LEFT JOIN CTE_LOCATION                                lc
    ON  lc.equnr = eq.equnr

LEFT JOIN CTE_EQUIPMENT_STATUS                        es
    ON  es.objnr = eq.objnr
    
LEFT JOIN CTE_FUNCLOC_DESC                            fd
    ON  fd.tplnr = lc.functional_location
    
LEFT JOIN CTE_S070_AGG                                s0
    ON  s0.equnr = eq.equnr

ORDER BY
    eq.werk    ASC,
    eq.eqtyp   ASC,
    eq.equnr   ASC;

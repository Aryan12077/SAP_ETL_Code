/* ============================================================
   IW29 — Display PM Notifications with Cost & Root Cause
   PostgreSQL (ECC)                                         v3
   Source schema: sap_stg_ecc   (extractor IW29 v1.3.0 ECC)

   CHANGE LOG (search for ">>> CHANGED" to find each edit)
     v3-1. CTE_PMCO_ACTUALS : wrttp = '04' (was '4', which never
                              matched, so actual cost was always 0)
     v3-2. CTE_PMCO_ACTUALS : NULLIF(TRIM(..),'') before the numeric
                              cast (blank WRTnn no longer errors)
     v3-3. CTE_PMCO_ACTUALS : UGX amounts x100 (0-decimal currency,
                              RFC returns amount / 100)
     v3-4. CTE_PMCO_ACTUALS : only order objects (objnr LIKE 'OR%')
     v3-5. CTE_NOTIF_STATUS : DISTINCT (objnr, stat) before aggregation
     v3-6. "Downtime"       : AUSZT is seconds; converted to the unit
                              in MAUEH (H/STD /3600, MIN /60,
                              TAG/D /86400, else seconds)
     v3-7. "Repair Cost (Est)" / "(Claimed)" : UGX x100 (still TEXT)
     v3-8. Optional planning-plant filter (commented out, see end)

   Output columns: unchanged (same 26 names, same order).

   STILL TO CONFIRM ON ECC
     - TCURX: UGX has CURRDEC = 0 (basis for x100).
     - "Root Cause" = QMFE.FECOD of item 0001 (damage code). The SAP
       cause code is QMUR.URCOD, which is not extracted. On HANA this
       had no source data; kept as is.
   ============================================================ */

WITH CTE_PMCO_ACTUALS AS (
    SELECT
        SUBSTRING(pmco.objnr FROM 3) AS order_number,
        pmco.cocur AS currency,

        SUM(
            /* >>> CHANGED (v3-2): blank-safe cast */
            NULLIF(TRIM(unpivot.cost), '')::NUMERIC
            /* >>> CHANGED (v3-3): UGX is a 0-decimal currency */
            * CASE WHEN TRIM(pmco.cocur) = 'UGX' THEN 100 ELSE 1 END
        ) AS total_actual_cost

    FROM sap_stg_ecc.stg_sap_iw29_pmco AS pmco

    CROSS JOIN LATERAL (
        VALUES
            (0,  pmco.wrt00),
            (1,  pmco.wrt01),
            (2,  pmco.wrt02),
            (3,  pmco.wrt03),
            (4,  pmco.wrt04),
            (5,  pmco.wrt05),
            (6,  pmco.wrt06),
            (7,  pmco.wrt07),
            (8,  pmco.wrt08),
            (9,  pmco.wrt09),
            (10, pmco.wrt10),
            (11, pmco.wrt11),
            (12, pmco.wrt12),
            (13, pmco.wrt13),
            (14, pmco.wrt14),
            (15, pmco.wrt15),
            (16, pmco.wrt16)
    ) AS unpivot(period, cost)

    /* >>> CHANGED (v3-1): was wrttp = '4' */
    WHERE TRIM(pmco.wrttp) = '04'
      /* >>> CHANGED (v3-4): order objects only */
      AND pmco.objnr LIKE 'OR%'
      AND NULLIF(TRIM(unpivot.cost), '') IS NOT NULL
      AND NULLIF(TRIM(unpivot.cost), '')::NUMERIC <> 0

    GROUP BY
        SUBSTRING(pmco.objnr FROM 3),
        pmco.cocur
),

CTE_NOTIF_STATUS AS (
    SELECT
        je.objnr,

        STRING_AGG(
            COALESCE(
                NULLIF(TRIM(tj.txt04), ''),
                je.stat
            ),
            ' '
            ORDER BY tj.txt04
        ) AS sys_status

    /* >>> CHANGED (v3-5): DISTINCT removes duplicate staging rows */
    FROM (
        SELECT DISTINCT objnr, stat
        FROM sap_stg_ecc.stg_sap_iw29_jest
        WHERE COALESCE(TRIM(inact), '') = ''
    ) AS je

    LEFT JOIN sap_stg_ecc.stg_sap_iw29_tj02t AS tj
        ON tj.istat = je.stat
       AND tj.spras = 'E'

    GROUP BY
        je.objnr
),

CTE_EQUIPMENT_DESC AS (
    SELECT
        eqkt.equnr,
        eqkt.eqktx AS equipment_desc

    FROM sap_stg_ecc.stg_sap_iw29_eqkt AS eqkt

    WHERE eqkt.spras = 'E'
),

CTE_ROOT_CAUSE AS (
    SELECT
        qmfe.qmnum,
        qmfe.fecod AS root_cause

    FROM sap_stg_ecc.stg_sap_iw29_qmfe AS qmfe

    WHERE qmfe.fenum = '0001'
)

/* ============================================================
   MAIN SELECT
   ============================================================ */

SELECT
    -- Planning / Notification
    h.iwerk                          AS "Planning Plant",
    q.qmnum                          AS "Notification",
    q.qmart                          AS "Notification Type",
    q.ernam                          AS "Created by",
    q.erzeit                         AS "Creation Time",
    q.qmdat                          AS "Notification Date",
    q.qmtxt                          AS "Description",

    -- Equipment
    h.equnr                          AS "Equipment",
    ed.equipment_desc                AS "Equipment Description",

    -- Planning Group / Order
    h.ingrp                          AS "PG",
    q.aufnr                          AS "Order",

    -- System Status
    ns.sys_status                    AS "System Status",

    -- Breakdown / Malfunction
    h.msaus                          AS "Breakdown Indicator",
    h.ausvn                          AS "Malfunction Start Date",
    h.auztv                          AS "Malfunction Start Time",
    h.ausbs                          AS "Malfunction End Date",
    h.auztb                          AS "Malfunction End Time",

    /* >>> CHANGED (v3-6): seconds -> unit in MAUEH (was always /3600) */
    ROUND(
        NULLIF(TRIM(h.auszt), '')::NUMERIC
        / CASE UPPER(TRIM(COALESCE(h.maueh, '')))
              WHEN 'H'   THEN 3600
              WHEN 'STD' THEN 3600
              WHEN 'MIN' THEN 60
              WHEN 'TAG' THEN 86400
              WHEN 'D'   THEN 86400
              ELSE 1
          END,
        2
    )                                AS "Downtime",
    h.maueh                          AS "Downtime Unit",

    -- Cost Information
    COALESCE(
        c.currency,
        q.waers
    )                                AS "Currency",

    /* >>> CHANGED (v3-7): UGX x100. Result cast back to TEXT so the
           column type in the final table does not change. */
    (NULLIF(TRIM(q.estimated_costs), '')::NUMERIC
        * CASE WHEN TRIM(q.waers) = 'UGX' THEN 100 ELSE 1 END)::TEXT
                                     AS "Repair Cost (Est)",
    (NULLIF(TRIM(q.claimed_costs), '')::NUMERIC
        * CASE WHEN TRIM(q.waers) = 'UGX' THEN 100 ELSE 1 END)::TEXT
                                     AS "Repair Cost (Claimed)",

    COALESCE(
        c.total_actual_cost,
        0::numeric
    )                                AS "Repair Cost (Actual)",

    -- Root Cause
    rc.root_cause                    AS "Root Cause",

    -- Functional Location
    l.tplnr                          AS "FunctLoc",
    l.beber                          AS "PlantSection"

FROM sap_stg_ecc.stg_sap_iw29_qmel AS q

/* PM Notification Extension */
LEFT JOIN sap_stg_ecc.stg_sap_iw29_qmih AS h
    ON h.qmnum = q.qmnum

/* Functional Location / Location Assignment */
LEFT JOIN sap_stg_ecc.stg_sap_iw29_iloa AS l
    ON l.iloan = h.iloan

/* Aggregated System Status */
LEFT JOIN CTE_NOTIF_STATUS AS ns
    ON ns.objnr = q.objnr

/* Equipment Description */
LEFT JOIN CTE_EQUIPMENT_DESC AS ed
    ON ed.equnr = h.equnr

/* Root Cause */
LEFT JOIN CTE_ROOT_CAUSE AS rc
    ON rc.qmnum = q.qmnum

/* Actual Maintenance Cost */
LEFT JOIN CTE_PMCO_ACTUALS AS c
    ON c.order_number = q.aufnr

/* >>> CHANGED (v3-8): optional. Uncomment to drop notifications with no
       planning plant (e.g. F3 quality notifications), as recommended on HANA. */
-- WHERE COALESCE(TRIM(h.iwerk), '') <> ''

ORDER BY
    q.qmnum;
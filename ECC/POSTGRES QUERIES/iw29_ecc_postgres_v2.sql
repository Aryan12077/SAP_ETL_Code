/* ============================================================
   IW29 — Display PM Notifications with Cost & Root Cause
   PostgreSQL
   ============================================================ */

WITH CTE_PMCO_ACTUALS AS (
    SELECT
        SUBSTRING(pmco.objnr FROM 3) AS order_number,
        pmco.cocur AS currency,
        SUM(unpivot.cost::numeric) AS total_actual_cost

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

    WHERE pmco.wrttp = '4'
      AND unpivot.cost IS NOT NULL
      AND unpivot.cost::numeric <> 0

    GROUP BY
        SUBSTRING(pmco.objnr FROM 3),
        pmco.cocur
),

CTE_NOTIF_STATUS AS (
    SELECT
        je.objnr,

        STRING_AGG(
            COALESCE(
                NULLIF(tj.txt04, ''),
                je.stat
            ),
            ' '
            ORDER BY tj.txt04
        ) AS sys_status

    FROM sap_stg_ecc.stg_sap_iw29_jest AS je

    LEFT JOIN sap_stg_ecc.stg_sap_iw29_tj02t AS tj
        ON tj.istat = je.stat
       AND tj.spras = 'E'

    WHERE je.inact = ''

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
    ROUND( h.auszt::numeric / 3600,2) AS "Downtime",
    h.maueh                          AS "Downtime Unit",

    -- Cost Information
    COALESCE(
        c.currency,
        q.waers
    )                                AS "Currency",

    q.estimated_costs                AS "Repair Cost (Est)",
    q.claimed_costs                  AS "Repair Cost (Claimed)",

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

ORDER BY
    q.qmnum;
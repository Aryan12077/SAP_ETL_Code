/* ============================================================
   IW29 — Display PM Notifications | PostgreSQL
   ============================================================ */

WITH CTE_NOTIF_STATUS AS (
    SELECT
        je.objnr,
        
        -- IW29 concatenates active statuses with a space (e.g., 'NOPR ORAS')
        STRING_AGG(
            COALESCE(NULLIF(tj.txt04, ''), je.stat),
            ' ' 
            ORDER BY tj.txt04
        )                                               AS sys_status

    FROM "sap_stg_ecc"."stg_sap_iw29_jest"              je
    LEFT JOIN "sap_stg_ecc"."stg_sap_iw29_tj02t"        tj
        ON  tj.istat = je.stat
        AND tj.spras = 'E'
    WHERE je.inact = ''
    GROUP BY je.objnr
),

CTE_EQUIPMENT_DESC AS (
    SELECT
        equnr,
        eqktx                                           AS equipment_desc
    FROM "sap_stg_ecc"."stg_sap_iw29_eqkt"
    WHERE spras = 'E'
)

/* ──────────────────────────────────────────────────────────────
   MAIN SELECT (Mapped to IW29 HTML Report Columns)
   ────────────────────────────────────────────────────────────── */
SELECT
    -- PM Planning Data (Sourced from QMIH)
    h.iwerk                                             AS "PlPl",
    
    -- Core Notification Header (Sourced from QMEL)
    q.qmnum                                             AS "Notifctn",
    q.ernam                                             AS "Created by",
    q.erzeit                                            AS "CreatTme",
    q.qmdat                                             AS "Notif.date",
    q.qmtxt                                             AS "Description",
    
    -- Equipment (Sourced from QMIH natively, no zero-stripping)
    h.equnr                                             AS "Equipment",
    ed.equipment_desc                                   AS "Equipment Description",
    
    h.ingrp                                             AS "PG",
    q.aufnr                                             AS "Order",
    
    -- Aggregated Status
    ns.sys_status                                       AS "SysStatus",
    
    -- Malfunction & Breakdown Data (Sourced from QMIH)
    h.msaus                                             AS "B",
    h.ausvn                                             AS "Malf.start",
    h.auztv                                             AS "MalfStrt",
    h.ausbs                                             AS "MalfEnd",
    h.auztb                                             AS "Malf.end",
    
    -- Location Account Assignment (Sourced from ILOA)
    l.tplnr                                             AS "FunctLoc",
    l.beber                                             AS "PlantSection"

FROM "sap_stg_ecc"."stg_sap_iw29_qmel"                  q

-- Join 1:1 PM extension table for plant, equipment, and breakdown flags
LEFT JOIN "sap_stg_ecc"."stg_sap_iw29_qmih"             h
    ON h.qmnum = q.qmnum

-- Join Location assignment table via the pointer in QMIH
LEFT JOIN "sap_stg_ecc"."stg_sap_iw29_iloa"             l
    ON l.iloan = h.iloan

-- Join Aggregated Status CTE
LEFT JOIN CTE_NOTIF_STATUS                              ns
    ON ns.objnr = q.objnr

-- Join Equipment Description CTE (Optional enrichment from Pass 4)
LEFT JOIN CTE_EQUIPMENT_DESC                            ed
    ON ed.equnr = h.equnr

ORDER BY
    q.qmnum ASC;
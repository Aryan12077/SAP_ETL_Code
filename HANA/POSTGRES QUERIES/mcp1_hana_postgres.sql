-- WITH CTE_WORK_CENTER AS (
--     SELECT
--         objid,
--         arbpl   AS work_center_name,
--         werks   AS wc_plant,
--         verwe   AS wc_category
--     FROM (
--         SELECT
--             objid,
--             arbpl,
--             werks,
--             verwe,
--             ROW_NUMBER() OVER (
--                 PARTITION BY objid
--                 ORDER BY
--                     CASE WHEN NULLIF(endda,'') IS NULL
--                               OR endda >= TO_CHAR(CURRENT_DATE, 'YYYYMMDD')
--                          THEN 0 ELSE 1 END,
--                     COALESCE(NULLIF(begda,''), '00000000') DESC
--             ) AS rn
--         FROM "sap_stg_hana"."stg_sap_mcp1_crhd"
--         WHERE COALESCE(lvorm, '') = ''
--     ) sub
--     WHERE rn = 1
-- ),

-- CTE_ACTUAL_CONFIRMATIONS AS (
--     SELECT
--         aufpl,
--         aplzl,
--         vornr,

--         SUM(
--             CASE 
--                 WHEN ismnu = 'MIN' THEN CAST(NULLIF(ismnw, '') AS NUMERIC(18,6)) / 480.0
--                 WHEN ismnu IN ('H', 'HR') THEN CAST(NULLIF(ismnw, '') AS NUMERIC(18,6)) / 8.0
--                 WHEN ismnu IN ('TAG', 'DAY', 'D') THEN CAST(NULLIF(ismnw, '') AS NUMERIC(18,6))
--                 ELSE CAST(NULLIF(ismnw, '') AS NUMERIC(18,6))
--             END
--         ) AS act_work_days_total,
--         SUM(
--             CASE 
--                 WHEN idaue = 'MIN' THEN CAST(NULLIF(idaur, '') AS NUMERIC(18,6)) / 480.0
--                 WHEN idaue IN ('H', 'HR') THEN CAST(NULLIF(idaur, '') AS NUMERIC(18,6)) / 8.0
--                 WHEN idaue IN ('TAG', 'DAY', 'D') THEN CAST(NULLIF(idaur, '') AS NUMERIC(18,6))
--                 ELSE CAST(NULLIF(idaur, '') AS NUMERIC(18,6))
--             END
--         ) AS act_leadtm_days_total,

--         SUM(CAST(NULLIF(gmnga, '') AS NUMERIC(18,3)))       AS conf_yield_qty,
--         SUM(CAST(NULLIF(lmnga, '') AS NUMERIC(18,3)))       AS conf_scrap_qty,

--         COUNT(*)                                             AS conf_count,
--         MAX(NULLIF(budat, ''))                               AS last_conf_date,
--         MIN(NULLIF(budat, ''))                               AS first_conf_date

--     FROM "sap_stg_hana"."stg_sap_mcp1_afru"
--     WHERE COALESCE(stokz, '') = ''
--     GROUP BY aufpl, aplzl, vornr
-- ),

-- CTE_PLANNED_TIMES_BASE AS (
--     SELECT
--         aufpl,
--         aplzl,

--         CAST(NULLIF(mgvrg, '') AS NUMERIC(18,3))            AS op_qty_planned,
--         meinh                                                AS op_uom,
--         CAST(NULLIF(lmnga, '') AS NUMERIC(18,3))            AS op_qty_confirmed,

--         CASE WHEN beaze = 'MIN' THEN CAST(NULLIF(bearz, '') AS NUMERIC(18,6)) / 480.0 WHEN beaze IN ('H', 'HR') THEN CAST(NULLIF(bearz, '') AS NUMERIC(18,6)) / 8.0 WHEN beaze IN ('TAG', 'DAY', 'D') THEN CAST(NULLIF(bearz, '') AS NUMERIC(18,6)) ELSE CAST(NULLIF(bearz, '') AS NUMERIC(18,6)) END AS proc_time_days,
--         CASE WHEN rstze = 'MIN' THEN CAST(NULLIF(ruest, '') AS NUMERIC(18,6)) / 480.0 WHEN rstze IN ('H', 'HR') THEN CAST(NULLIF(ruest, '') AS NUMERIC(18,6)) / 8.0 WHEN rstze IN ('TAG', 'DAY', 'D') THEN CAST(NULLIF(ruest, '') AS NUMERIC(18,6)) ELSE CAST(NULLIF(ruest, '') AS NUMERIC(18,6)) END AS setup_time_days,
--         CASE WHEN aruze = 'MIN' THEN CAST(NULLIF(abrue, '') AS NUMERIC(18,6)) / 480.0 WHEN aruze IN ('H', 'HR') THEN CAST(NULLIF(abrue, '') AS NUMERIC(18,6)) / 8.0 WHEN aruze IN ('TAG', 'DAY', 'D') THEN CAST(NULLIF(abrue, '') AS NUMERIC(18,6)) ELSE CAST(NULLIF(abrue, '') AS NUMERIC(18,6)) END AS teardown_time_days,
--         CASE WHEN ligze = 'MIN' THEN CAST(NULLIF(liegz, '') AS NUMERIC(18,6)) / 480.0 WHEN ligze IN ('H', 'HR') THEN CAST(NULLIF(liegz, '') AS NUMERIC(18,6)) / 8.0 WHEN ligze IN ('TAG', 'DAY', 'D') THEN CAST(NULLIF(liegz, '') AS NUMERIC(18,6)) ELSE CAST(NULLIF(liegz, '') AS NUMERIC(18,6)) END AS wait_time_days,
--         CASE WHEN traze = 'MIN' THEN CAST(NULLIF(tranz, '') AS NUMERIC(18,6)) / 480.0 WHEN traze IN ('H', 'HR') THEN CAST(NULLIF(tranz, '') AS NUMERIC(18,6)) / 8.0 WHEN traze IN ('TAG', 'DAY', 'D') THEN CAST(NULLIF(tranz, '') AS NUMERIC(18,6)) ELSE CAST(NULLIF(tranz, '') AS NUMERIC(18,6)) END AS move_time_days,

--         beaze   AS proc_time_unit,
--         rstze   AS setup_time_unit,
--         aruze   AS teardown_time_unit,
--         ligze   AS wait_time_unit,
--         traze   AS move_time_unit
--     FROM "sap_stg_hana"."stg_sap_mcp1_afvv"
-- ),

-- CTE_PLANNED_TIMES AS (
--     SELECT 
--         *,
--         COALESCE(proc_time_days, 0) + COALESCE(setup_time_days, 0) + COALESCE(teardown_time_days, 0) + COALESCE(wait_time_days, 0) + COALESCE(move_time_days, 0) AS tgt_leadtm_days
--     FROM CTE_PLANNED_TIMES_BASE
-- ),

-- CTE_OPER_COUNT AS (
--     SELECT
--         aufpl,
--         COUNT(*)                                   AS total_operations,
--         SUM(CASE WHEN COALESCE(loekz,'') = ''
--                  THEN 1 ELSE 0 END)               AS active_operations
--     FROM "sap_stg_hana"."stg_sap_mcp1_afvc"
--     GROUP BY aufpl
-- ),

-- CTE_ORDER_LEADTIME_SUMMARY AS (
--     SELECT
--         ko.aufnr,

--         oc.total_operations,
--         oc.active_operations,

--         SUM(pt.tgt_leadtm_days)           AS order_tgt_leadtm_days,
--         SUM(ac.act_leadtm_days_total)     AS order_act_leadtm_days,

--         CASE
--             WHEN SUM(pt.tgt_leadtm_days) IS NULL
--               OR SUM(pt.tgt_leadtm_days) = 0 THEN NULL
--             ELSE ROUND(
--                 100.0 * (SUM(ac.act_leadtm_days_total) - SUM(pt.tgt_leadtm_days))
--                 / SUM(pt.tgt_leadtm_days),
--                 2
--             )
--         END AS order_leadtm_variance_pct

--     FROM "sap_stg_hana"."stg_sap_mcp1_afko" ko
--     LEFT JOIN CTE_OPER_COUNT        oc ON oc.aufpl  = ko.aufpl
--     LEFT JOIN "sap_stg_hana"."stg_sap_mcp1_afvc" vc
--         ON vc.aufpl = ko.aufpl
--         AND COALESCE(vc.loekz,'') = ''
--     LEFT JOIN CTE_PLANNED_TIMES     pt ON pt.aufpl  = vc.aufpl
--                                       AND pt.aplzl  = vc.aplzl
--     LEFT JOIN CTE_ACTUAL_CONFIRMATIONS ac
--         ON ac.aufpl = vc.aufpl
--         AND ac.aplzl = vc.aplzl
--     GROUP BY ko.aufnr, oc.total_operations, oc.active_operations
-- )

-- /* ══════════════════════════════════════════════════════════════
--     MAIN SELECT
--    ══════════════════════════════════════════════════════════════ */
-- SELECT

--     ak.aufnr                                    AS "Order",
--     ak.auart                                    AS "Order Type",
--     COALESCE(NULLIF(t003p.txt, ''), ak.auart)   AS "Order Type Text",

--     ak.ktext                                    AS "Order Description",
--     ak.werks                                    AS "Plant",
--     ak.prctr                                    AS "Profit Center",
--     ak.kostl                                    AS "Cost Center",
--     ak.erdat                                    AS "Created on",

--     ko.gstrp                                    AS "Bsc Start",
--     ko.gltrp                                    AS "Basic Fin.",
--     ko.gstrs                                    AS "Sched. Start",
--     ko.gltrs                                    AS "Sched. Finish",
--     ko.gstri                                    AS "Actual Start",
--     ko.gltri                                    AS "Actual Finish",
--     ko.fevor                                    AS "Prod. Supervisor",
--     ko.plgrp                                    AS "Planner Group",

--     CAST(NULLIF(ko.gamng, '') AS NUMERIC(18,3)) AS "Total Order Qty",
--     ko.gmein                                    AS "Header UoM",

--     ols.total_operations                        AS "No. of Oper./Act.",
--     ols.active_operations                       AS "Active Operations",

--     ROUND(ols.order_tgt_leadtm_days, 2)         AS "Order TgtLeadTm (DAY)",
--     ROUND(ols.order_act_leadtm_days, 2)         AS "Order ActLeadTm (DAY)",
--     ols.order_leadtm_variance_pct               AS "Order LeadTm Var. %",

--     vc.vornr                                    AS "Oper./Act.",
--     vc.ltxa1                                    AS "Operation Text",
--     vc.steus                                    AS "Control Key",
--     vc.loekz                                    AS "Oper. Del. Flag",

--     CASE vc.loekz
--         WHEN 'X' THEN 'Deleted'
--         WHEN ''  THEN 'Active'
--         ELSE          'Active'
--     END                                         AS "Oper. Status",

--     wc.work_center_name                         AS "Work Center",
--     wc.wc_plant                                 AS "WC Plant",
--     wc.wc_category                              AS "WC Category",
--     vc.arbid                                    AS "WC Object ID",

--     pt.op_qty_planned                           AS "Op. Qty Planned",
--     pt.op_uom                                   AS "Op. UoM",
--     pt.op_qty_confirmed                         AS "Op. Qty Confirmed",

--     pt.proc_time_days                           AS "Proc. Time (DAY)",
--     pt.setup_time_days                          AS "Setup Time (DAY)",
--     pt.teardown_time_days                       AS "Teardown Time (DAY)",
--     pt.wait_time_days                           AS "Wait Time (DAY)",
--     pt.move_time_days                           AS "Move Time (DAY)",
--     ROUND(pt.tgt_leadtm_days, 2)               AS "TgtLeadTm. (DAY)",

--     pt.proc_time_unit                           AS "Proc. Time Unit",
--     pt.setup_time_unit                          AS "Setup Time Unit",
--     pt.teardown_time_unit                       AS "Teardown Time Unit",
--     pt.wait_time_unit                           AS "Wait Time Unit",
--     pt.move_time_unit                           AS "Move Time Unit",

--     ROUND(ac.act_work_days_total,    2)         AS "Act. Work (DAY)",
--     ROUND(ac.act_leadtm_days_total,  2)         AS "ActLeadTm. (DAY)",
--     ac.conf_count                               AS "Confirmations",
--     ac.conf_yield_qty                           AS "Conf. Yield Qty",
--     ac.conf_scrap_qty                           AS "Conf. Scrap Qty",
--     ac.first_conf_date                          AS "First Conf. Date",
--     ac.last_conf_date                           AS "Last Conf. Date",

--     ROUND(
--         ac.act_leadtm_days_total - pt.tgt_leadtm_days,
--         2
--     )                                           AS "LeadTm Variance (DAY)",

--     CASE
--         WHEN pt.tgt_leadtm_days IS NULL
--           OR pt.tgt_leadtm_days = 0 THEN NULL
--         ELSE ROUND(
--             100.0 * (ac.act_leadtm_days_total - pt.tgt_leadtm_days)
--             / pt.tgt_leadtm_days,
--             2
--         )
--     END                                         AS "LeadTm Variance %",

--     CASE
--         WHEN ac.act_leadtm_days_total IS NULL           THEN 'No Actuals'
--         WHEN pt.tgt_leadtm_days       IS NULL           THEN 'No Plan'
--         WHEN ac.act_leadtm_days_total <= pt.tgt_leadtm_days
--                                                         THEN 'On / Under Plan'
--         WHEN ac.act_leadtm_days_total <= pt.tgt_leadtm_days * 1.25
--                                                         THEN '< 25% Over Plan'
--         WHEN ac.act_leadtm_days_total <= pt.tgt_leadtm_days * 1.50
--                                                         THEN '25–50% Over Plan'
--         ELSE                                                 '> 50% Over Plan'
--     END                                         AS "Lead Time Band"

-- FROM "sap_stg_hana"."stg_sap_mcp1_aufk" ak

-- LEFT JOIN "sap_stg_hana"."stg_sap_mcp1_afko" ko
--     ON  ko.aufnr = ak.aufnr

-- LEFT JOIN "sap_stg_hana"."stg_sap_mcp1_afvc" vc
--     ON  vc.aufpl = ko.aufpl

-- LEFT JOIN CTE_PLANNED_TIMES pt
--     ON  pt.aufpl = vc.aufpl
--     AND pt.aplzl = vc.aplzl

-- LEFT JOIN CTE_ACTUAL_CONFIRMATIONS ac
--     ON  ac.aufpl = vc.aufpl
--     AND ac.aplzl = vc.aplzl

-- LEFT JOIN CTE_WORK_CENTER wc
--     ON  wc.objid = vc.arbid

-- LEFT JOIN CTE_ORDER_LEADTIME_SUMMARY ols
--     ON  ols.aufnr = ak.aufnr

-- LEFT JOIN "sap_stg_hana"."stg_sap_mcp1_t003p" t003p
--     ON  t003p.auart = ak.auart
--     AND t003p.spras = 'E'

-- ORDER BY
--     ak.erdat       DESC,
--     ak.aufnr       ASC,
--     vc.vornr       ASC;


--new code as on 13-11-2026
/* ============================================================
    MCP1 — Operation Analysis (SFIS, info structure S022) | PostgreSQL   (v3)

    CHANGE LOG (search for ">>> CHANGED" to find each edit)
      v2-1. CTE_S022 (new)      : SAP's own lead-time key figures from info structure S022
                                  (one row per order operation; joined on order + activity).
      v2-2. "TgtLeadTm. (DAY)"  : S022.S_DFZ (target execution time, factory calendar)
                                  instead of (proc + setup + teardown + wait + move) / 8 h.
                                  Example order 1033124 / 0010: was 20.00, SAP shows 22.2.
      v2-3. "ActLeadTm. (DAY)"  : S022.I_DFZ (actual execution time) instead of AFRU.IDAUR.
      v2-4. Order-level lead times, variances and band now use the S022 values.
      v2-5. "Lead Time Band"    : 'No Actuals' also when the operation has no actual
                                  execution start (S022.ISDD blank), not only when NULL.
      v2-6. "Conf. Scrap Qty"   : S022.XMNGA (scrap). Was SUM(AFRU.LMNGA), which is YIELD
                                  (order 1033124 showed scrap 504 = yield 504).
      v2-7. CTE_ACTUAL_CONFIRMATIONS : reversal documents excluded (AFRU.STZHL filled).
      v2-8. CTE_WORK_CENTER     : validity no longer depends on CURRENT_DATE; restricted to
                                  object type 'A' (work centre).
      v3-1. "Op. UoM"           : S022.MEINH first (unit SAP/MCP1 uses), AFVV.MEINH as
                                  fallback. Order 1034201 (PH01): AFVV had EA, S022 has M,
                                  which put 41,520 yield under EA instead of M.

    Output columns: UNCHANGED (same 55 names, same order, same types).

    PREREQUISITES (extractor MCP1_Production_Operations.py v1.1.0):
      - staging table sap_stg_hana.stg_sap_mcp1_s022 (incl. column meinh)
      - column stzhl in sap_stg_hana.stg_sap_mcp1_afru

    NOT CHANGED (business definitions, documented):
      - "Proc./Setup/Teardown/Wait/Move Time (DAY)" still convert AFVV standard values
        with 480 min (8 h) = 1 day.
      - "Act. Work (DAY)" still from AFRU.ISMNW with 8 h = 1 day.
      - Lead Time Band thresholds 1.25 / 1.50.
   ============================================================ */

WITH CTE_WORK_CENTER AS (
    SELECT
        objid,
        arbpl   AS work_center_name,
        werks   AS wc_plant,
        verwe   AS wc_category
    FROM (
        SELECT
            objid,
            arbpl,
            werks,
            verwe,
            ROW_NUMBER() OVER (
                PARTITION BY objid
                /* >>> CHANGED (v2-8): was ordered by validity against CURRENT_DATE,
                       so the result depended on the day the query ran.
                       Now: latest valid-to, then latest valid-from. */
                ORDER BY
                    COALESCE(NULLIF(endda, ''), '99991231') DESC,
                    COALESCE(NULLIF(begda, ''), '00000000') DESC
            ) AS rn
        FROM "sap_stg_hana"."stg_sap_mcp1_crhd"
        WHERE COALESCE(lvorm, '') = ''
          /* >>> CHANGED (v2-8): work centres only */
          AND COALESCE(NULLIF(objty, ''), 'A') = 'A'
    ) sub
    WHERE rn = 1
),

/* >>> CHANGED (v2-1): new CTE. S022 = SFIS order operation data (source of MCP1).
       One row per order operation (key includes date; DISTINCT ON guards against
       an operation appearing on more than one date: latest date wins). */
CTE_S022 AS (
    SELECT DISTINCT ON (aufnr, vornr)
        aufnr,
        vornr,

        /* >>> CHANGED (v3-1): operation unit as recorded by SAP in S022 */
        NULLIF(TRIM(meinh), '')                                   AS op_uom,

        /* Target execution time, unit DFZEH (internal 'TAG' = DAY) */
        CASE
            WHEN UPPER(TRIM(dfzeh)) IN ('TAG', 'DAY', 'D') THEN CAST(NULLIF(TRIM(s_dfz), '') AS NUMERIC(18,6))
            WHEN UPPER(TRIM(dfzeh)) IN ('H', 'HR', 'STD')  THEN CAST(NULLIF(TRIM(s_dfz), '') AS NUMERIC(18,6)) / 8.0
            WHEN UPPER(TRIM(dfzeh)) = 'MIN'                THEN CAST(NULLIF(TRIM(s_dfz), '') AS NUMERIC(18,6)) / 480.0
            ELSE CAST(NULLIF(TRIM(s_dfz), '') AS NUMERIC(18,6))
        END                                                       AS tgt_exec_days,

        /* Actual execution time, same unit */
        CASE
            WHEN UPPER(TRIM(dfzeh)) IN ('TAG', 'DAY', 'D') THEN CAST(NULLIF(TRIM(i_dfz), '') AS NUMERIC(18,6))
            WHEN UPPER(TRIM(dfzeh)) IN ('H', 'HR', 'STD')  THEN CAST(NULLIF(TRIM(i_dfz), '') AS NUMERIC(18,6)) / 8.0
            WHEN UPPER(TRIM(dfzeh)) = 'MIN'                THEN CAST(NULLIF(TRIM(i_dfz), '') AS NUMERIC(18,6)) / 480.0
            ELSE CAST(NULLIF(TRIM(i_dfz), '') AS NUMERIC(18,6))
        END                                                       AS act_exec_days,

        CAST(NULLIF(TRIM(xmnga), '') AS NUMERIC(18,3))            AS scrap_qty,

        /* Actual execution start: blank / 00000000 = not started */
        CASE
            WHEN COALESCE(NULLIF(TRIM(isdd), ''), '00000000') = '00000000' THEN 0
            ELSE 1
        END                                                       AS has_actual_start

    FROM "sap_stg_hana"."stg_sap_mcp1_s022"
    WHERE vrsio = '000'
      AND COALESCE(TRIM(ssour), '') = ''
    ORDER BY aufnr, vornr, sptag DESC
),

CTE_ACTUAL_CONFIRMATIONS AS (
    SELECT
        aufpl,
        aplzl,
        vornr,

        SUM(
            CASE 
                WHEN ismnu = 'MIN' THEN CAST(NULLIF(ismnw, '') AS NUMERIC(18,6)) / 480.0
                WHEN ismnu IN ('H', 'HR') THEN CAST(NULLIF(ismnw, '') AS NUMERIC(18,6)) / 8.0
                WHEN ismnu IN ('TAG', 'DAY', 'D') THEN CAST(NULLIF(ismnw, '') AS NUMERIC(18,6))
                ELSE CAST(NULLIF(ismnw, '') AS NUMERIC(18,6))
            END
        ) AS act_work_days_total,

        SUM(CAST(NULLIF(gmnga, '') AS NUMERIC(18,3)))       AS conf_yield_qty,

        COUNT(*)                                             AS conf_count,
        MAX(NULLIF(budat, ''))                               AS last_conf_date,
        MIN(NULLIF(budat, ''))                               AS first_conf_date

    FROM "sap_stg_hana"."stg_sap_mcp1_afru"
    WHERE COALESCE(stokz, '') = ''
      /* >>> CHANGED (v2-7): a reversal document has STZHL = counter of the confirmation
             it cancels; normal confirmations have it blank / 00000000. */
      AND COALESCE(NULLIF(TRIM(stzhl), ''), '00000000') = '00000000'
    GROUP BY aufpl, aplzl, vornr
),

CTE_PLANNED_TIMES_BASE AS (
    SELECT
        aufpl,
        aplzl,

        CAST(NULLIF(mgvrg, '') AS NUMERIC(18,3))            AS op_qty_planned,
        meinh                                                AS op_uom,
        CAST(NULLIF(lmnga, '') AS NUMERIC(18,3))            AS op_qty_confirmed,

        CASE WHEN beaze = 'MIN' THEN CAST(NULLIF(bearz, '') AS NUMERIC(18,6)) / 480.0 WHEN beaze IN ('H', 'HR') THEN CAST(NULLIF(bearz, '') AS NUMERIC(18,6)) / 8.0 WHEN beaze IN ('TAG', 'DAY', 'D') THEN CAST(NULLIF(bearz, '') AS NUMERIC(18,6)) ELSE CAST(NULLIF(bearz, '') AS NUMERIC(18,6)) END AS proc_time_days,
        CASE WHEN rstze = 'MIN' THEN CAST(NULLIF(ruest, '') AS NUMERIC(18,6)) / 480.0 WHEN rstze IN ('H', 'HR') THEN CAST(NULLIF(ruest, '') AS NUMERIC(18,6)) / 8.0 WHEN rstze IN ('TAG', 'DAY', 'D') THEN CAST(NULLIF(ruest, '') AS NUMERIC(18,6)) ELSE CAST(NULLIF(ruest, '') AS NUMERIC(18,6)) END AS setup_time_days,
        CASE WHEN aruze = 'MIN' THEN CAST(NULLIF(abrue, '') AS NUMERIC(18,6)) / 480.0 WHEN aruze IN ('H', 'HR') THEN CAST(NULLIF(abrue, '') AS NUMERIC(18,6)) / 8.0 WHEN aruze IN ('TAG', 'DAY', 'D') THEN CAST(NULLIF(abrue, '') AS NUMERIC(18,6)) ELSE CAST(NULLIF(abrue, '') AS NUMERIC(18,6)) END AS teardown_time_days,
        CASE WHEN ligze = 'MIN' THEN CAST(NULLIF(liegz, '') AS NUMERIC(18,6)) / 480.0 WHEN ligze IN ('H', 'HR') THEN CAST(NULLIF(liegz, '') AS NUMERIC(18,6)) / 8.0 WHEN ligze IN ('TAG', 'DAY', 'D') THEN CAST(NULLIF(liegz, '') AS NUMERIC(18,6)) ELSE CAST(NULLIF(liegz, '') AS NUMERIC(18,6)) END AS wait_time_days,
        CASE WHEN traze = 'MIN' THEN CAST(NULLIF(tranz, '') AS NUMERIC(18,6)) / 480.0 WHEN traze IN ('H', 'HR') THEN CAST(NULLIF(tranz, '') AS NUMERIC(18,6)) / 8.0 WHEN traze IN ('TAG', 'DAY', 'D') THEN CAST(NULLIF(tranz, '') AS NUMERIC(18,6)) ELSE CAST(NULLIF(tranz, '') AS NUMERIC(18,6)) END AS move_time_days,

        beaze   AS proc_time_unit,
        rstze   AS setup_time_unit,
        aruze   AS teardown_time_unit,
        ligze   AS wait_time_unit,
        traze   AS move_time_unit
    FROM "sap_stg_hana"."stg_sap_mcp1_afvv"
),

CTE_OPER_COUNT AS (
    SELECT
        aufpl,
        COUNT(*)                                   AS total_operations,
        SUM(CASE WHEN COALESCE(loekz,'') = ''
                 THEN 1 ELSE 0 END)               AS active_operations
    FROM "sap_stg_hana"."stg_sap_mcp1_afvc"
    GROUP BY aufpl
),

CTE_ORDER_LEADTIME_SUMMARY AS (
    SELECT
        ko.aufnr,

        oc.total_operations,
        oc.active_operations,

        /* >>> CHANGED (v2-4): order totals now summed from S022 values */
        SUM(s22.tgt_exec_days)            AS order_tgt_leadtm_days,
        SUM(s22.act_exec_days)            AS order_act_leadtm_days,

        CASE
            WHEN SUM(s22.tgt_exec_days) IS NULL
              OR SUM(s22.tgt_exec_days) = 0 THEN NULL
            ELSE ROUND(
                100.0 * (SUM(s22.act_exec_days) - SUM(s22.tgt_exec_days))
                / SUM(s22.tgt_exec_days),
                2
            )
        END AS order_leadtm_variance_pct

    FROM "sap_stg_hana"."stg_sap_mcp1_afko" ko
    LEFT JOIN CTE_OPER_COUNT        oc ON oc.aufpl  = ko.aufpl
    LEFT JOIN "sap_stg_hana"."stg_sap_mcp1_afvc" vc
        ON vc.aufpl = ko.aufpl
        AND COALESCE(vc.loekz,'') = ''
    LEFT JOIN CTE_S022 s22
        ON s22.aufnr = ko.aufnr
        AND s22.vornr = vc.vornr
    GROUP BY ko.aufnr, oc.total_operations, oc.active_operations
)

/* ══════════════════════════════════════════════════════════════
    MAIN SELECT
   ══════════════════════════════════════════════════════════════ */
SELECT

    ak.aufnr                                    AS "Order",
    ak.auart                                    AS "Order Type",
    COALESCE(NULLIF(t003p.txt, ''), ak.auart)   AS "Order Type Text",

    ak.ktext                                    AS "Order Description",
    ak.werks                                    AS "Plant",
    ak.prctr                                    AS "Profit Center",
    ak.kostl                                    AS "Cost Center",
    ak.erdat                                    AS "Created on",

    ko.gstrp                                    AS "Bsc Start",
    ko.gltrp                                    AS "Basic Fin.",
    ko.gstrs                                    AS "Sched. Start",
    ko.gltrs                                    AS "Sched. Finish",
    ko.gstri                                    AS "Actual Start",
    ko.gltri                                    AS "Actual Finish",
    ko.fevor                                    AS "Prod. Supervisor",
    ko.plgrp                                    AS "Planner Group",

    CAST(NULLIF(ko.gamng, '') AS NUMERIC(18,3)) AS "Total Order Qty",
    ko.gmein                                    AS "Header UoM",

    ols.total_operations                        AS "No. of Oper./Act.",
    ols.active_operations                       AS "Active Operations",

    ROUND(ols.order_tgt_leadtm_days, 2)         AS "Order TgtLeadTm (DAY)",
    ROUND(ols.order_act_leadtm_days, 2)         AS "Order ActLeadTm (DAY)",
    ols.order_leadtm_variance_pct               AS "Order LeadTm Var. %",

    vc.vornr                                    AS "Oper./Act.",
    vc.ltxa1                                    AS "Operation Text",
    vc.steus                                    AS "Control Key",
    vc.loekz                                    AS "Oper. Del. Flag",

    CASE vc.loekz
        WHEN 'X' THEN 'Deleted'
        WHEN ''  THEN 'Active'
        ELSE          'Active'
    END                                         AS "Oper. Status",

    wc.work_center_name                         AS "Work Center",
    wc.wc_plant                                 AS "WC Plant",
    wc.wc_category                              AS "WC Category",
    vc.arbid                                    AS "WC Object ID",

    pt.op_qty_planned                           AS "Op. Qty Planned",
    /* >>> CHANGED (v3-1): unit from S022 (as MCP1 groups it), AFVV as fallback.
           Order 1034201: AFVV said EA, SAP S022 says M. */
    COALESCE(s22.op_uom, pt.op_uom)             AS "Op. UoM",
    pt.op_qty_confirmed                         AS "Op. Qty Confirmed",

    pt.proc_time_days                           AS "Proc. Time (DAY)",
    pt.setup_time_days                          AS "Setup Time (DAY)",
    pt.teardown_time_days                       AS "Teardown Time (DAY)",
    pt.wait_time_days                           AS "Wait Time (DAY)",
    pt.move_time_days                           AS "Move Time (DAY)",

    /* >>> CHANGED (v2-2): SAP target execution time (S022.S_DFZ) */
    ROUND(s22.tgt_exec_days, 2)                 AS "TgtLeadTm. (DAY)",

    pt.proc_time_unit                           AS "Proc. Time Unit",
    pt.setup_time_unit                          AS "Setup Time Unit",
    pt.teardown_time_unit                       AS "Teardown Time Unit",
    pt.wait_time_unit                           AS "Wait Time Unit",
    pt.move_time_unit                           AS "Move Time Unit",

    ROUND(ac.act_work_days_total,    2)         AS "Act. Work (DAY)",

    /* >>> CHANGED (v2-3): SAP actual execution time (S022.I_DFZ) */
    ROUND(s22.act_exec_days, 2)                 AS "ActLeadTm. (DAY)",

    ac.conf_count                               AS "Confirmations",
    ac.conf_yield_qty                           AS "Conf. Yield Qty",

    /* >>> CHANGED (v2-6): scrap from S022.XMNGA (was SUM(AFRU.LMNGA) = yield) */
    s22.scrap_qty                               AS "Conf. Scrap Qty",

    ac.first_conf_date                          AS "First Conf. Date",
    ac.last_conf_date                           AS "Last Conf. Date",

    /* >>> CHANGED (v2-4): variance from S022 values */
    ROUND(
        s22.act_exec_days - s22.tgt_exec_days,
        2
    )                                           AS "LeadTm Variance (DAY)",

    CASE
        WHEN s22.tgt_exec_days IS NULL
          OR s22.tgt_exec_days = 0 THEN NULL
        ELSE ROUND(
            100.0 * (s22.act_exec_days - s22.tgt_exec_days)
            / s22.tgt_exec_days,
            2
        )
    END                                         AS "LeadTm Variance %",

    /* >>> CHANGED (v2-4, v2-5): band from S022; 0 without actual start = No Actuals */
    CASE
        WHEN s22.act_exec_days IS NULL
          OR COALESCE(s22.has_actual_start, 0) = 0      THEN 'No Actuals'
        WHEN s22.tgt_exec_days IS NULL
          OR s22.tgt_exec_days = 0                      THEN 'No Plan'
        WHEN s22.act_exec_days <= s22.tgt_exec_days
                                                        THEN 'On / Under Plan'
        WHEN s22.act_exec_days <= s22.tgt_exec_days * 1.25
                                                        THEN '< 25% Over Plan'
        WHEN s22.act_exec_days <= s22.tgt_exec_days * 1.50
                                                        THEN '25–50% Over Plan'
        ELSE                                                 '> 50% Over Plan'
    END                                         AS "Lead Time Band"

FROM "sap_stg_hana"."stg_sap_mcp1_aufk" ak

LEFT JOIN "sap_stg_hana"."stg_sap_mcp1_afko" ko
    ON  ko.aufnr = ak.aufnr

LEFT JOIN "sap_stg_hana"."stg_sap_mcp1_afvc" vc
    ON  vc.aufpl = ko.aufpl

LEFT JOIN CTE_PLANNED_TIMES_BASE pt
    ON  pt.aufpl = vc.aufpl
    AND pt.aplzl = vc.aplzl

LEFT JOIN CTE_ACTUAL_CONFIRMATIONS ac
    ON  ac.aufpl = vc.aufpl
    AND ac.aplzl = vc.aplzl

/* >>> CHANGED (v2-1): S022 key figures per operation */
LEFT JOIN CTE_S022 s22
    ON  s22.aufnr = ak.aufnr
    AND s22.vornr = vc.vornr

LEFT JOIN CTE_WORK_CENTER wc
    ON  wc.objid = vc.arbid

LEFT JOIN CTE_ORDER_LEADTIME_SUMMARY ols
    ON  ols.aufnr = ak.aufnr

LEFT JOIN "sap_stg_hana"."stg_sap_mcp1_t003p" t003p
    ON  t003p.auart = ak.auart
    AND t003p.spras = 'E'

ORDER BY
    ak.erdat       DESC,
    ak.aufnr       ASC,
    vc.vornr       ASC;
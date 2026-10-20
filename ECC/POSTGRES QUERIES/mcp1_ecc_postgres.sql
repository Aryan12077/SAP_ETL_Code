/* ============================================================
    MCP1 — Production Order Operations & Lead Time Report | PostgreSQL
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
                ORDER BY
                    CASE WHEN NULLIF(endda,'') IS NULL
                              OR endda >= TO_CHAR(CURRENT_DATE, 'YYYYMMDD')
                         THEN 0 ELSE 1 END,
                    COALESCE(NULLIF(begda,''), '00000000') DESC
            ) AS rn
        FROM "sap_stg_ecc"."stg_sap_mcp1_crhd"
        WHERE COALESCE(lvorm, '') = ''
    ) sub
    WHERE rn = 1
),

CTE_ACTUAL_CONFIRMATIONS AS (
    SELECT
        aufpl,
        aplzl,
        vornr,

        SUM(CAST(NULLIF(ismnw_day, '') AS NUMERIC(18,6)))   AS act_work_days_total,
        SUM(CAST(NULLIF(act_leadtm_day, '') AS NUMERIC(18,6))) AS act_leadtm_days_total,

        SUM(CAST(NULLIF(gmnga, '') AS NUMERIC(18,3)))       AS conf_yield_qty,
        SUM(CAST(NULLIF(lmnga, '') AS NUMERIC(18,3)))       AS conf_scrap_qty,

        COUNT(*)                                             AS conf_count,
        MAX(NULLIF(budat, ''))                               AS last_conf_date,
        MIN(NULLIF(budat, ''))                               AS first_conf_date

    FROM "sap_stg_ecc"."stg_sap_mcp1_afru"
    WHERE COALESCE(stokz, '') = ''
    GROUP BY aufpl, aplzl, vornr
),

CTE_PLANNED_TIMES AS (
    SELECT
        aufpl,
        aplzl,

        CAST(NULLIF(mgvrg, '') AS NUMERIC(18,3))            AS op_qty_planned,
        meinh                                                AS op_uom,
        CAST(NULLIF(lmnga, '') AS NUMERIC(18,3))            AS op_qty_confirmed,

        CAST(NULLIF(bearz_day,  '') AS NUMERIC(18,6))       AS proc_time_days,
        CAST(NULLIF(ruest_day,  '') AS NUMERIC(18,6))       AS setup_time_days,
        CAST(NULLIF(abrue_day,  '') AS NUMERIC(18,6))       AS teardown_time_days,
        CAST(NULLIF(liegz_day,  '') AS NUMERIC(18,6))       AS wait_time_days,
        CAST(NULLIF(tranz_day,  '') AS NUMERIC(18,6))       AS move_time_days,
        CAST(NULLIF(tgt_leadtm_day,'') AS NUMERIC(18,6))    AS tgt_leadtm_days,

        beaze   AS proc_time_unit,
        rstze   AS setup_time_unit,
        aruze   AS teardown_time_unit,
        ligze   AS wait_time_unit,
        traze   AS move_time_unit
    FROM "sap_stg_ecc"."stg_sap_mcp1_afvv"
),

CTE_OPER_COUNT AS (
    SELECT
        aufpl,
        COUNT(*)                                   AS total_operations,
        SUM(CASE WHEN COALESCE(loekz,'') = ''
                 THEN 1 ELSE 0 END)               AS active_operations
    FROM "sap_stg_ecc"."stg_sap_mcp1_afvc"
    GROUP BY aufpl
),

CTE_ORDER_LEADTIME_SUMMARY AS (
    SELECT
        ko.aufnr,

        oc.total_operations,
        oc.active_operations,

        SUM(pt.tgt_leadtm_days)           AS order_tgt_leadtm_days,
        SUM(ac.act_leadtm_days_total)     AS order_act_leadtm_days,

        CASE
            WHEN SUM(pt.tgt_leadtm_days) IS NULL
              OR SUM(pt.tgt_leadtm_days) = 0 THEN NULL
            ELSE ROUND(
                100.0 * (SUM(ac.act_leadtm_days_total) - SUM(pt.tgt_leadtm_days))
                / SUM(pt.tgt_leadtm_days),
                2
            )
        END AS order_leadtm_variance_pct

    FROM "sap_stg_ecc"."stg_sap_mcp1_afko" ko
    LEFT JOIN CTE_OPER_COUNT        oc ON oc.aufpl  = ko.aufpl
    LEFT JOIN "sap_stg_ecc"."stg_sap_mcp1_afvc" vc
        ON vc.aufpl = ko.aufpl
        AND COALESCE(vc.loekz,'') = ''
    LEFT JOIN CTE_PLANNED_TIMES     pt ON pt.aufpl  = vc.aufpl
                                      AND pt.aplzl  = vc.aplzl
    LEFT JOIN CTE_ACTUAL_CONFIRMATIONS ac
        ON ac.aufpl = vc.aufpl
        AND ac.aplzl = vc.aplzl
    GROUP BY ko.aufnr, oc.total_operations, oc.active_operations
)

/* ══════════════════════════════════════════════════════════════
    MAIN SELECT
   ══════════════════════════════════════════════════════════════ */
SELECT

    ak.aufnr                                    AS "Order",
    ak.auart                                    AS "Order Type",

    CASE ak.auart
        WHEN 'PP01' THEN 'Standard Production'
        WHEN 'PP02' THEN 'Repetitive Manufacturing'
        WHEN 'PP03' THEN 'Campaign Order'
        WHEN 'PM01' THEN 'Maintenance Order'
        WHEN 'SM01' THEN 'Service Order'
        ELSE ak.auart
    END                                         AS "Order Type Text",

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
    pt.op_uom                                   AS "Op. UoM",
    pt.op_qty_confirmed                         AS "Op. Qty Confirmed",

    pt.proc_time_days                           AS "Proc. Time (DAY)",
    pt.setup_time_days                          AS "Setup Time (DAY)",
    pt.teardown_time_days                       AS "Teardown Time (DAY)",
    pt.wait_time_days                           AS "Wait Time (DAY)",
    pt.move_time_days                           AS "Move Time (DAY)",
    ROUND(pt.tgt_leadtm_days, 2)               AS "TgtLeadTm. (DAY)",

    pt.proc_time_unit                           AS "Proc. Time Unit",
    pt.setup_time_unit                          AS "Setup Time Unit",
    pt.teardown_time_unit                       AS "Teardown Time Unit",
    pt.wait_time_unit                           AS "Wait Time Unit",
    pt.move_time_unit                           AS "Move Time Unit",

    ROUND(ac.act_work_days_total,    2)         AS "Act. Work (DAY)",
    ROUND(ac.act_leadtm_days_total,  2)         AS "ActLeadTm. (DAY)",
    ac.conf_count                               AS "Confirmations",
    ac.conf_yield_qty                           AS "Conf. Yield Qty",
    ac.conf_scrap_qty                           AS "Conf. Scrap Qty",
    ac.first_conf_date                          AS "First Conf. Date",
    ac.last_conf_date                           AS "Last Conf. Date",

    ROUND(
        ac.act_leadtm_days_total - pt.tgt_leadtm_days,
        2
    )                                           AS "LeadTm Variance (DAY)",

    CASE
        WHEN pt.tgt_leadtm_days IS NULL
          OR pt.tgt_leadtm_days = 0 THEN NULL
        ELSE ROUND(
            100.0 * (ac.act_leadtm_days_total - pt.tgt_leadtm_days)
            / pt.tgt_leadtm_days,
            2
        )
    END                                         AS "LeadTm Variance %",

    CASE
        WHEN ac.act_leadtm_days_total IS NULL           THEN 'No Actuals'
        WHEN pt.tgt_leadtm_days       IS NULL           THEN 'No Plan'
        WHEN ac.act_leadtm_days_total <= pt.tgt_leadtm_days
                                                        THEN 'On / Under Plan'
        WHEN ac.act_leadtm_days_total <= pt.tgt_leadtm_days * 1.25
                                                        THEN '< 25% Over Plan'
        WHEN ac.act_leadtm_days_total <= pt.tgt_leadtm_days * 1.50
                                                        THEN '25–50% Over Plan'
        ELSE                                                 '> 50% Over Plan'
    END                                         AS "Lead Time Band"

FROM "sap_stg_ecc"."stg_sap_mcp1_aufk" ak

LEFT JOIN "sap_stg_ecc"."stg_sap_mcp1_afko" ko
    ON  ko.aufnr = ak.aufnr

LEFT JOIN "sap_stg_ecc"."stg_sap_mcp1_afvc" vc
    ON  vc.aufpl = ko.aufpl

LEFT JOIN CTE_PLANNED_TIMES pt
    ON  pt.aufpl = vc.aufpl
    AND pt.aplzl = vc.aplzl

LEFT JOIN CTE_ACTUAL_CONFIRMATIONS ac
    ON  ac.aufpl = vc.aufpl
    AND ac.aplzl = vc.aplzl

LEFT JOIN CTE_WORK_CENTER wc
    ON  wc.objid = vc.arbid

LEFT JOIN CTE_ORDER_LEADTIME_SUMMARY ols
    ON  ols.aufnr = ak.aufnr

ORDER BY
    ak.erdat       DESC,
    ak.aufnr       ASC,
    vc.vornr       ASC;

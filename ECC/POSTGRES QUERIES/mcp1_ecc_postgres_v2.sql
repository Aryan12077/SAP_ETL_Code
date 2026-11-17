/* ============================================================
    MCP1 — Production Order Operations & Lead Time Report
    PostgreSQL (ECC)                                         v2
    Source schema: sap_stg_ecc   (extractor MCP1 v1.1.0 ECC:
                                  AUTYP '10'/'40', AFRU.STZHL, S022)

    Hybrid design (same as the validated HANA v3):
      descriptive columns from AUFK/AFKO/AFVC/AFVV/CRHD,
      lead times / scrap / UoM from LIS info structure S022 (version 000).

    CHANGE LOG (search for ">>> CHANGED" to find each edit)
      v2-1. *_day columns removed. They were never created in staging
            (no post_process hook), so v1 failed with "column does not
            exist". Day values are now calculated inline from the unit:
            480 min = 1 day (business definition, unchanged).
      v2-2. CTE_S022 (new): target / actual execution time (S_DFZ / I_DFZ),
            scrap (XMNGA), actual start (ISDD), UoM (MEINH).
      v2-3. "TgtLeadTm. (DAY)"  = S022.S_DFZ  (was sum of standard values)
      v2-4. "ActLeadTm. (DAY)"  = S022.I_DFZ
      v2-5. Order-level lead times, variances and band from S022
      v2-6. "Lead Time Band" = 'No Actuals' when no actual execution start
      v2-7. "Conf. Scrap Qty"  = S022.XMNGA (was SUM(AFRU.LMNGA) = yield)
      v2-8. "Conf. Yield Qty"  = SUM(AFRU.LMNGA) (was GMNGA)
      v2-9. AFRU reversal documents excluded (STZHL <> 0), as well as
            reversed confirmations (STOKZ = 'X')
      v2-10. Work centre: CRHD object type 'A', no CURRENT_DATE dependency
      v2-11. "Order Type Text" from T003P (was a hardcoded CASE)
      v2-12. "Op. UoM" = COALESCE(S022.MEINH, AFVV.MEINH)

    Unchanged business definitions: Proc./Setup/Teardown/Wait/Move Time
    and Act. Work in days use 8 h = 480 min = 1 day; band thresholds
    1.25 / 1.50.

    Output columns: unchanged (same names, same order, same types).

    STILL TO CONFIRM ON ECC
      - S022 is updated on ECC (SE16N S022, version 000, a few orders).
      - S022.DFZEH units: expect TAG only (query below handles H/MIN too).
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
            /* >>> CHANGED (v2-10): latest validity wins; no CURRENT_DATE */
            ROW_NUMBER() OVER (
                PARTITION BY objid
                ORDER BY
                    COALESCE(NULLIF(TRIM(endda), ''), '99991231') DESC,
                    COALESCE(NULLIF(TRIM(begda), ''), '00000000') DESC
            ) AS rn
        FROM "sap_stg_ecc"."stg_sap_mcp1_crhd"
        WHERE COALESCE(TRIM(lvorm), '') = ''
          /* >>> CHANGED (v2-10): work centres only */
          AND COALESCE(NULLIF(TRIM(objty), ''), 'A') = 'A'
    ) sub
    WHERE rn = 1
),

/* >>> CHANGED (v2-11): new CTE, replaces the hardcoded order type CASE */
CTE_ORDER_TYPE_TEXT AS (
    SELECT DISTINCT ON (auart)
        auart,
        NULLIF(TRIM(txt), '')   AS order_type_text
    FROM "sap_stg_ecc"."stg_sap_mcp1_t003p"
    WHERE spras = 'E'
    ORDER BY auart
),

CTE_ACTUAL_CONFIRMATIONS AS (
    SELECT
        aufpl,
        aplzl,
        vornr,

        /* >>> CHANGED (v2-1): actual work converted from ISMNU (8 h = 1 day) */
        SUM(
            CAST(NULLIF(TRIM(ismnw), '') AS NUMERIC(18,6))
            / CASE UPPER(TRIM(COALESCE(ismnu, '')))
                  WHEN 'MIN' THEN 480.0
                  WHEN 'H'   THEN 8.0
                  WHEN 'STD' THEN 8.0
                  WHEN 'S'   THEN 28800.0
                  WHEN 'SEC' THEN 28800.0
                  ELSE 1.0                      -- TAG / D / blank
              END
        )                                                    AS act_work_days_total,

        /* >>> CHANGED (v2-8): LMNGA is the confirmed yield */
        SUM(CAST(NULLIF(TRIM(lmnga), '') AS NUMERIC(18,3)))  AS conf_yield_qty,

        COUNT(*)                                             AS conf_count,
        MAX(NULLIF(budat, ''))                               AS last_conf_date,
        MIN(NULLIF(budat, ''))                               AS first_conf_date

    FROM "sap_stg_ecc"."stg_sap_mcp1_afru"
    WHERE COALESCE(TRIM(stokz), '') = ''
      /* >>> CHANGED (v2-9): exclude reversal documents themselves */
      AND COALESCE(NULLIF(TRIM(stzhl), ''), '00000000') = '00000000'
    GROUP BY aufpl, aplzl, vornr
),

/* >>> CHANGED (v2-2): new CTE. S022 = SFIS order operation data (MCP1's source).
       DISTINCT ON guards against an operation appearing on more than one date. */
CTE_S022 AS (
    SELECT DISTINCT ON (aufnr, vornr)
        aufnr,
        vornr,
        NULLIF(TRIM(meinh), '')                                   AS op_uom,

        CASE
            WHEN UPPER(TRIM(dfzeh)) IN ('TAG','DAY','D') THEN CAST(NULLIF(TRIM(s_dfz),'') AS NUMERIC(18,6))
            WHEN UPPER(TRIM(dfzeh)) IN ('H','HR','STD')  THEN CAST(NULLIF(TRIM(s_dfz),'') AS NUMERIC(18,6)) / 8.0
            WHEN UPPER(TRIM(dfzeh)) = 'MIN'              THEN CAST(NULLIF(TRIM(s_dfz),'') AS NUMERIC(18,6)) / 480.0
            ELSE CAST(NULLIF(TRIM(s_dfz),'') AS NUMERIC(18,6))
        END                                                       AS tgt_exec_days,

        CASE
            WHEN UPPER(TRIM(dfzeh)) IN ('TAG','DAY','D') THEN CAST(NULLIF(TRIM(i_dfz),'') AS NUMERIC(18,6))
            WHEN UPPER(TRIM(dfzeh)) IN ('H','HR','STD')  THEN CAST(NULLIF(TRIM(i_dfz),'') AS NUMERIC(18,6)) / 8.0
            WHEN UPPER(TRIM(dfzeh)) = 'MIN'              THEN CAST(NULLIF(TRIM(i_dfz),'') AS NUMERIC(18,6)) / 480.0
            ELSE CAST(NULLIF(TRIM(i_dfz),'') AS NUMERIC(18,6))
        END                                                       AS act_exec_days,

        CAST(NULLIF(TRIM(xmnga),'') AS NUMERIC(18,3))             AS scrap_qty,

        CASE WHEN COALESCE(NULLIF(TRIM(isdd),''),'00000000') = '00000000'
             THEN 0 ELSE 1 END                                    AS has_actual_start

    FROM "sap_stg_ecc"."stg_sap_mcp1_s022"
    WHERE vrsio = '000'
      AND COALESCE(TRIM(ssour), '') = ''
    ORDER BY aufnr, vornr, sptag DESC
),

CTE_PLANNED_TIMES AS (
    SELECT
        aufpl,
        aplzl,

        CAST(NULLIF(TRIM(mgvrg), '') AS NUMERIC(18,3))      AS op_qty_planned,
        meinh                                                AS op_uom_afvv,
        CAST(NULLIF(TRIM(lmnga), '') AS NUMERIC(18,3))      AS op_qty_confirmed,

        /* >>> CHANGED (v2-1): days calculated here (480 min = 1 day) */
        CAST(CAST(NULLIF(TRIM(bearz), '') AS NUMERIC(18,6))
             / CASE UPPER(TRIM(COALESCE(beaze, ''))) WHEN 'MIN' THEN 480.0 WHEN 'H' THEN 8.0 WHEN 'STD' THEN 8.0
                    WHEN 'S' THEN 28800.0 WHEN 'SEC' THEN 28800.0 ELSE 1.0 END
             AS NUMERIC(18,6))                               AS proc_time_days,
        CAST(CAST(NULLIF(TRIM(ruest), '') AS NUMERIC(18,6))
             / CASE UPPER(TRIM(COALESCE(rstze, ''))) WHEN 'MIN' THEN 480.0 WHEN 'H' THEN 8.0 WHEN 'STD' THEN 8.0
                    WHEN 'S' THEN 28800.0 WHEN 'SEC' THEN 28800.0 ELSE 1.0 END
             AS NUMERIC(18,6))                               AS setup_time_days,
        CAST(CAST(NULLIF(TRIM(abrue), '') AS NUMERIC(18,6))
             / CASE UPPER(TRIM(COALESCE(aruze, ''))) WHEN 'MIN' THEN 480.0 WHEN 'H' THEN 8.0 WHEN 'STD' THEN 8.0
                    WHEN 'S' THEN 28800.0 WHEN 'SEC' THEN 28800.0 ELSE 1.0 END
             AS NUMERIC(18,6))                               AS teardown_time_days,
        CAST(CAST(NULLIF(TRIM(liegz), '') AS NUMERIC(18,6))
             / CASE UPPER(TRIM(COALESCE(ligze, ''))) WHEN 'MIN' THEN 480.0 WHEN 'H' THEN 8.0 WHEN 'STD' THEN 8.0
                    WHEN 'S' THEN 28800.0 WHEN 'SEC' THEN 28800.0 ELSE 1.0 END
             AS NUMERIC(18,6))                               AS wait_time_days,
        CAST(CAST(NULLIF(TRIM(tranz), '') AS NUMERIC(18,6))
             / CASE UPPER(TRIM(COALESCE(traze, ''))) WHEN 'MIN' THEN 480.0 WHEN 'H' THEN 8.0 WHEN 'STD' THEN 8.0
                    WHEN 'S' THEN 28800.0 WHEN 'SEC' THEN 28800.0 ELSE 1.0 END
             AS NUMERIC(18,6))                               AS move_time_days,

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

/* >>> CHANGED (v2-5): order-level lead times from S022 */
CTE_ORDER_LEADTIME_SUMMARY AS (
    SELECT
        ko.aufnr,

        oc.total_operations,
        oc.active_operations,

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

    FROM "sap_stg_ecc"."stg_sap_mcp1_afko" ko
    LEFT JOIN CTE_OPER_COUNT        oc ON oc.aufpl  = ko.aufpl
    LEFT JOIN "sap_stg_ecc"."stg_sap_mcp1_afvc" vc
        ON vc.aufpl = ko.aufpl
        AND COALESCE(vc.loekz,'') = ''
    LEFT JOIN CTE_S022              s22
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

    /* >>> CHANGED (v2-11) */
    COALESCE(ot.order_type_text, ak.auart)      AS "Order Type Text",

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
        ELSE          'Active'
    END                                         AS "Oper. Status",

    wc.work_center_name                         AS "Work Center",
    wc.wc_plant                                 AS "WC Plant",
    wc.wc_category                              AS "WC Category",
    vc.arbid                                    AS "WC Object ID",

    pt.op_qty_planned                           AS "Op. Qty Planned",
    /* >>> CHANGED (v2-12) */
    COALESCE(s22.op_uom, pt.op_uom_afvv)        AS "Op. UoM",
    pt.op_qty_confirmed                         AS "Op. Qty Confirmed",

    pt.proc_time_days                           AS "Proc. Time (DAY)",
    pt.setup_time_days                          AS "Setup Time (DAY)",
    pt.teardown_time_days                       AS "Teardown Time (DAY)",
    pt.wait_time_days                           AS "Wait Time (DAY)",
    pt.move_time_days                           AS "Move Time (DAY)",
    /* >>> CHANGED (v2-3) */
    ROUND(s22.tgt_exec_days, 2)                 AS "TgtLeadTm. (DAY)",

    pt.proc_time_unit                           AS "Proc. Time Unit",
    pt.setup_time_unit                          AS "Setup Time Unit",
    pt.teardown_time_unit                       AS "Teardown Time Unit",
    pt.wait_time_unit                           AS "Wait Time Unit",
    pt.move_time_unit                           AS "Move Time Unit",

    ROUND(ac.act_work_days_total, 2)            AS "Act. Work (DAY)",
    /* >>> CHANGED (v2-4) */
    ROUND(s22.act_exec_days, 2)                 AS "ActLeadTm. (DAY)",
    ac.conf_count                               AS "Confirmations",
    ac.conf_yield_qty                           AS "Conf. Yield Qty",
    /* >>> CHANGED (v2-7) */
    s22.scrap_qty::NUMERIC                      AS "Conf. Scrap Qty",
    ac.first_conf_date                          AS "First Conf. Date",
    ac.last_conf_date                           AS "Last Conf. Date",

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

    /* >>> CHANGED (v2-6) */
    CASE
        WHEN s22.act_exec_days IS NULL
          OR COALESCE(s22.has_actual_start, 0) = 0           THEN 'No Actuals'
        WHEN s22.tgt_exec_days IS NULL
          OR s22.tgt_exec_days = 0                           THEN 'No Plan'
        WHEN s22.act_exec_days <= s22.tgt_exec_days          THEN 'On / Under Plan'
        WHEN s22.act_exec_days <= s22.tgt_exec_days * 1.25   THEN '< 25% Over Plan'
        WHEN s22.act_exec_days <= s22.tgt_exec_days * 1.50   THEN '25–50% Over Plan'
        ELSE                                                      '> 50% Over Plan'
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

/* >>> CHANGED (v2-2) */
LEFT JOIN CTE_S022 s22
    ON  s22.aufnr = ak.aufnr
    AND s22.vornr = vc.vornr

LEFT JOIN CTE_WORK_CENTER wc
    ON  wc.objid = vc.arbid

LEFT JOIN CTE_ORDER_LEADTIME_SUMMARY ols
    ON  ols.aufnr = ak.aufnr

LEFT JOIN CTE_ORDER_TYPE_TEXT ot
    ON  ot.auart = ak.auart

ORDER BY
    ak.erdat       DESC,
    ak.aufnr       ASC,
    vc.vornr       ASC;
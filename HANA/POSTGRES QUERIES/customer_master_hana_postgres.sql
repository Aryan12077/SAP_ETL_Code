WITH CTE_KNVV AS (
    -- One sales area row per customer (mirrors the original
    -- drop_duplicates("KUNNR") behaviour after merging the two
    -- KNVV field-batches in pandas).
    SELECT DISTINCT ON (kunnr)
        kunnr,
        vkorg,
        vtweg,
        spart,
        waers,
        zterm,
        vkgrp,
        bzirk
    FROM "sap_stg_hana"."stg_sap_customer_master_knvv"
    ORDER BY kunnr, vkorg, vtweg, spart
),

CTE_KNB1 AS (
    -- Fan-out driver — ALL (KUNNR, BUKRS) pairs kept, no dedup by KUNNR.
    -- Each customer produces one output row per company code assigned.
    SELECT DISTINCT
        kunnr,
        bukrs AS company_code
    FROM "sap_stg_hana"."stg_sap_customer_master_knb1"
),

-- NEW: Employee Responsible lookup (KNVP PARVW = 'ZM' -> PA0002)
-- Catch: PERNR is only populated on KNVP rows where PARVW = 'ZM'
-- (confirmed against this system's actual KNVP data — 'ER' does not
-- exist here; 'ZM' is this client's configured Employee Responsible
-- code, matching the same code used for VBPA in the VA05 query),
-- so that filter is applied here before joining to PA0002.
CTE_EMP_RESP AS (
    SELECT DISTINCT ON (kv.kunnr)
        kv.kunnr,
        kv.pernr        AS employee_pernr,
        hr.vorna        AS employee_first_name,
        hr.nachn        AS employee_last_name
    FROM "sap_stg_hana"."stg_sap_customer_master_knvp" kv
    LEFT JOIN "sap_stg_hana"."stg_sap_customer_master_pa0002" hr
        ON hr.pernr = kv.pernr
    WHERE kv.parvw = 'ZH'
      AND kv.pernr IS NOT NULL
      AND kv.pernr <> ''
    ORDER BY kv.kunnr, kv.vkorg, kv.vtweg, kv.spart
)

SELECT
    /* ── Fan-out key ──────────────────────────────────────── */
    kb.company_code                              AS "company_code",

    /* ── Customer identity ───────────────────────────────── */
    kn.kunnr                                     AS "customer_code",
    kn.name1                                     AS "customer_name",

    /* ── Executive / Sales Group (unchanged logic: KNVV+TVGRT) */
    CASE
        WHEN sv.vkgrp IS NULL OR tv.bezei IN ('Blocked', 'X', 'Y', '01', '02')
            THEN ''
        ELSE sv.vkgrp
    END                                           AS "executive_id",
    CASE
        WHEN tv.bezei IN ('Blocked', 'X', 'Y', '01', '02')
            THEN ''
        ELSE COALESCE(tv.bezei, '')
    END                                           AS "executive",

    /* ── Category ─────────────────────────────────────────── */
    kn.ktokd                                     AS "category_code",
    kn.ktokd                                     AS "category",

    /* ── Zone / Area ──────────────────────────────────────── */
    sv.bzirk                                     AS "zone_code",
    sv.bzirk                                     AS "area",

    /* ── State / Region ──────────────────────────────────── */
    kn.regio                                     AS "state_code",
    COALESCE(t5u.bezei, '')                       AS "state",

    /* ── City ─────────────────────────────────────────────── */
    kn.cityc                                     AS "city_code",
    kn.ort01                                     AS "city",

    /* ── Country ──────────────────────────────────────────── */
    kn.land1                                     AS "country_code",
    COALESCE(t5t.landx, '')                       AS "country",

    /* ── Industry ─────────────────────────────────────────── */
    kn.brsch                                     AS "industry_id",
    COALESCE(t16.brtxt, '')                       AS "industry",

    /* ── Currency / Credit Terms ─────────────────────────── */
    sv.waers                                     AS "currency_code",
    sv.zterm                                     AS "credit_days",

    /* ── NEW: Employee Responsible (KNVP PARVW='ER' + PA0002) */
    COALESCE(er.employee_pernr, '')               AS "sales_employee_pernr",
    COALESCE(er.employee_first_name, '')          AS "employee_first_name",
    COALESCE(er.employee_last_name, '')           AS "employee_last_name"

FROM      "sap_stg_hana"."stg_sap_customer_master_kna1" kn
INNER JOIN CTE_KNB1                kb  ON kb.kunnr = kn.kunnr   -- fan-out
LEFT JOIN CTE_KNVV                 sv  ON sv.kunnr = kn.kunnr
LEFT JOIN "sap_stg_hana"."stg_sap_customer_master_t005t" t5t ON t5t.land1 = kn.land1
LEFT JOIN "sap_stg_hana"."stg_sap_customer_master_t005u" t5u ON t5u.land1 = kn.land1 AND t5u.bland = kn.regio
LEFT JOIN "sap_stg_hana"."stg_sap_customer_master_t016t" t16 ON t16.brsch = kn.brsch
LEFT JOIN "sap_stg_hana"."stg_sap_customer_master_tvgrt" tv  ON tv.vkgrp = sv.vkgrp
LEFT JOIN CTE_EMP_RESP              er  ON er.kunnr = kn.kunnr

ORDER BY
    kb.company_code ASC,
    kn.kunnr         ASC;
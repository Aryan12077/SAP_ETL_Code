WITH

/* ──────────────────────────────────────────────────────────────
   CTE 1 — Product Group Descriptions (PRGRP codes from MAKT)
   ────────────────────────────────────────────────────────────── */
CTE_PRODUCT_GROUP_DESC AS (
    SELECT DISTINCT
        matnr                                       AS product_group,
        maktx                                       AS product_group_description
    FROM "sap_stg_hana"."stg_sap_matmas_makt"
    WHERE spras = 'E'
),

/* ──────────────────────────────────────────────────────────────
   CTE 2 — Material Descriptions (NRMIT codes from MAKT)
   ────────────────────────────────────────────────────────────── */
CTE_MATERIAL_DESC AS (
    SELECT DISTINCT
        matnr,
        maktx                                       AS material_description
    FROM "sap_stg_hana"."stg_sap_matmas_makt"
    WHERE spras = 'E'
),

/* ──────────────────────────────────────────────────────────────
   CTE 3 — Material Group Descriptions (T023T)
   ────────────────────────────────────────────────────────────── */
CTE_MATERIAL_GROUP_DESC AS (
    SELECT DISTINCT
        matkl,
        wgbez                                       AS material_group_desc
    FROM "sap_stg_hana"."stg_sap_matmas_t023t"
    WHERE spras = 'E'
),

/* ──────────────────────────────────────────────────────────────
   CTE 4 — Material Type Descriptions (T134T)
   ────────────────────────────────────────────────────────────── */
CTE_MATERIAL_TYPE_DESC AS (
    SELECT DISTINCT
        mtart,
        mtbez                                       AS material_type_desc
    FROM "sap_stg_hana"."stg_sap_matmas_t134t"
    WHERE spras = 'E'
),

/* ──────────────────────────────────────────────────────────────
   CTE 5 — MARA deduplicated (handles duplicate MATNR rows)
   ────────────────────────────────────────────────────────────── */
CTE_MARA AS (
    SELECT DISTINCT ON (matnr)
        matnr, mtart, matkl, meins, gewei,
        brgew, ntgew, volum, ersda, laeda, lvorm, prdha
    FROM "sap_stg_hana"."stg_sap_matmas_mara"
    ORDER BY matnr, ersda DESC
),

/* ──────────────────────────────────────────────────────────────
   CTE 6 — PGMI deduplicated (one row per PRGRP + NRMIT)
   ────────────────────────────────────────────────────────────── */
CTE_PGMI AS (
    SELECT DISTINCT
        prgrp,
        nrmit,
        werks,
        antei,
        datum
    FROM "sap_stg_hana"."stg_sap_matmas_pgmi"
),

/* ──────────────────────────────────────────────────────────────
   CTE 7 — Plant / MRP Data (MARC, aggregated per material)
   ────────────────────────────────────────────────────────────── */
CTE_PLANT_DATA AS (
    SELECT
        matnr,
        STRING_AGG(werks, ' | ' ORDER BY werks)         AS plants,
        COUNT(DISTINCT werks)                            AS plant_count,
        STRING_AGG(
            werks || ':' || COALESCE(NULLIF(minbe, ''), '0'),
            ' | ' ORDER BY werks
        )                                                AS reorder_level_by_plant,
        STRING_AGG(
            werks || ':' || COALESCE(NULLIF(eisbe, ''), '0'),
            ' | ' ORDER BY werks
        )                                                AS safety_stock_by_plant,
        STRING_AGG(
            werks || ':' || COALESCE(NULLIF(mabst, ''), '0'),
            ' | ' ORDER BY werks
        )                                                AS max_stock_by_plant,
        MIN(CAST(NULLIF(plifz, '') AS NUMERIC(10, 0)))  AS min_planned_delivery_days,
        MAX(CAST(NULLIF(plifz, '') AS NUMERIC(10, 0)))  AS max_planned_delivery_days,
        STRING_AGG(
            werks || ':' || COALESCE(NULLIF(plifz, ''), '0') || 'd',
            ' | ' ORDER BY werks
        )                                                AS delivery_time_by_plant
    FROM "sap_stg_hana"."stg_sap_matmas_marc"
    GROUP BY matnr
),

/* ──────────────────────────────────────────────────────────────
   CTE 8 — Material Valuation (MBEW, aggregated per material)
   ────────────────────────────────────────────────────────────── */
CTE_VALUATION AS (
    SELECT
        matnr,
        STRING_AGG(bwkey, ' | ' ORDER BY bwkey)         AS valuation_areas,
        COUNT(DISTINCT bwkey)                            AS valuation_area_count,
        MIN(CAST(NULLIF(verpr, '') AS NUMERIC(18, 5)))  AS moving_avg_price_min,
        MAX(CAST(NULLIF(verpr, '') AS NUMERIC(18, 5)))  AS moving_avg_price_max,
        MIN(CAST(NULLIF(stprs, '') AS NUMERIC(18, 5)))  AS standard_price_min,
        MAX(CAST(NULLIF(stprs, '') AS NUMERIC(18, 5)))  AS standard_price_max,
        STRING_AGG(
            bwkey || ':' || COALESCE(NULLIF(verpr, ''), '0'),
            ' | ' ORDER BY bwkey
        )                                                AS moving_avg_price_by_area,
        STRING_AGG(
            bwkey || ':' || COALESCE(NULLIF(stprs, ''), '0'),
            ' | ' ORDER BY bwkey
        )                                                AS standard_price_by_area
    FROM "sap_stg_hana"."stg_sap_matmas_mbew"
    GROUP BY matnr
),

/* ──────────────────────────────────────────────────────────────
   CTE 9 — Storage Location Data (MARD, aggregated per material)
   ────────────────────────────────────────────────────────────── */
CTE_STORAGE AS (
    SELECT
        matnr,
        STRING_AGG(
            werks || '/' || lgort,
            ' | ' ORDER BY werks, lgort
        )                                                AS storage_plant_sloc,
        COUNT(DISTINCT werks)                            AS storage_plant_count,
        COUNT(DISTINCT lgort)                            AS storage_location_count,
        COUNT(*)                                         AS storage_assignment_count
    FROM "sap_stg_hana"."stg_sap_matmas_mard"
    GROUP BY matnr
)

/* ──────────────────────────────────────────────────────────────
   MAIN SELECT — driven from PGMI (Product Group → Material)
   Materials without a product group assignment are excluded.
   Change INNER JOIN on CTE_MARA to LEFT JOIN if you want them.
   ────────────────────────────────────────────────────────────── */
SELECT

    /* ── Product Group (top level in this dataset) ───────────────────────── */
    pg.prgrp                                        AS "Product Group",
    COALESCE(NULLIF(pgd.product_group_description, ''), pg.prgrp)
                                                    AS "Product Group Description",

    /* ── Material ────────────────────────────────────────────────────────── */
    ma.matnr                                        AS "Material Number",
    COALESCE(NULLIF(md.material_description, ''), '— description not found —')
                                                    AS "Material Description",

    /* ── Material Type ───────────────────────────────────────────────────── */
    ma.mtart                                        AS "Material Type",
    COALESCE(NULLIF(tt.material_type_desc, ''), ma.mtart)
                                                    AS "Material Type Text",
    CASE ma.mtart
        WHEN 'FERT' THEN 'Finished Product'
        WHEN 'HALB' THEN 'Semi-Finished Product'
        WHEN 'ROH'  THEN 'Raw Material'
        WHEN 'HIBE' THEN 'Operating Supplies'
        WHEN 'HAWA' THEN 'Trading Goods'
        WHEN 'DIEN' THEN 'Services'
        WHEN 'NLAG' THEN 'Non-Stock Material'
        WHEN 'ERSA' THEN 'Spare Parts'
        ELSE COALESCE(NULLIF(tt.material_type_desc, ''), ma.mtart)
    END                                             AS "Material Type Category",

    /* ── Material Group ──────────────────────────────────────────────────── */
    ma.matkl                                        AS "Material Group",
    COALESCE(NULLIF(tg.material_group_desc, ''), ma.matkl)
                                                    AS "Material Group Text",

    /* ── Units of Measure & Physical Attributes ──────────────────────────── */
    ma.meins                                        AS "Base Unit of Measure",
    ma.gewei                                        AS "Weight Unit",
    CAST(NULLIF(ma.brgew, '') AS NUMERIC(18, 3))    AS "Gross Weight",
    CAST(NULLIF(ma.ntgew, '') AS NUMERIC(18, 3))    AS "Net Weight",
    CAST(NULLIF(ma.volum, '') AS NUMERIC(18, 3))    AS "Volume",

    /* ── Lifecycle Dates ─────────────────────────────────────────────────── */
    ma.ersda                                        AS "Created On",
    ma.laeda                                        AS "Last Changed On",

    /* ── Deletion Status ─────────────────────────────────────────────────── */
    ma.lvorm                                        AS "Cross-Plant Del. Flag",
    CASE ma.lvorm
        WHEN 'X' THEN 'Flagged for Deletion'
        ELSE 'Active'
    END                                             AS "Deletion Status",

    /* ── Product Group Assignment Detail ─────────────────────────────────── */
    pg.werks                                        AS "PG Assigned Plant",
    CAST(NULLIF(pg.antei, '') AS NUMERIC(18, 10))   AS "PG Proportion",
    pg.datum                                        AS "PG Valid From",

    /* ── Plant / MRP Data ────────────────────────────────────────────────── */
    pl.plants                                       AS "Plants",
    pl.plant_count                                  AS "Plant Count",
    pl.reorder_level_by_plant                       AS "Reorder Level (Plant:Qty)",
    pl.safety_stock_by_plant                        AS "Safety Stock (Plant:Qty)",
    pl.max_stock_by_plant                           AS "Max Stock Level (Plant:Qty)",
    pl.min_planned_delivery_days                    AS "Min Planned Delivery (Days)",
    pl.max_planned_delivery_days                    AS "Max Planned Delivery (Days)",
    pl.delivery_time_by_plant                       AS "Planned Delivery (Plant:Days)",

    /* ── Valuation ───────────────────────────────────────────────────────── */
    va.valuation_areas                              AS "Valuation Areas",
    va.valuation_area_count                         AS "Valuation Area Count",
    CASE
        WHEN va.moving_avg_price_min = va.moving_avg_price_max
        THEN va.moving_avg_price_min
        ELSE NULL
    END                                             AS "Moving Avg Price",
    CASE
        WHEN va.standard_price_min = va.standard_price_max
        THEN va.standard_price_min
        ELSE NULL
    END                                             AS "Standard Price",
    va.moving_avg_price_min                         AS "Moving Avg Price (Min)",
    va.moving_avg_price_max                         AS "Moving Avg Price (Max)",
    va.standard_price_min                           AS "Standard Price (Min)",
    va.standard_price_max                           AS "Standard Price (Max)",
    va.moving_avg_price_by_area                     AS "Moving Avg Price (Area:Price)",
    va.standard_price_by_area                       AS "Standard Price (Area:Price)",

    /* ── Storage Locations ───────────────────────────────────────────────── */
    st.storage_plant_sloc                           AS "Storage Plant/SLoc",
    st.storage_plant_count                          AS "Storage Plant Count",
    st.storage_location_count                       AS "Storage Location Count",
    st.storage_assignment_count                     AS "Storage Assignments",

    /* ── Coverage Flags ──────────────────────────────────────────────────── */
    CASE WHEN pl.matnr IS NOT NULL THEN 'Y' ELSE 'N' END  AS "Has Plant Data",
    CASE WHEN va.matnr IS NOT NULL THEN 'Y' ELSE 'N' END  AS "Has Valuation",
    CASE WHEN st.matnr IS NOT NULL THEN 'Y' ELSE 'N' END  AS "Has Storage Locs"

FROM CTE_PGMI pg

INNER JOIN CTE_MARA ma
    ON ma.matnr = pg.nrmit

LEFT JOIN CTE_PRODUCT_GROUP_DESC pgd
    ON pgd.product_group = pg.prgrp

LEFT JOIN CTE_MATERIAL_DESC md
    ON md.matnr = ma.matnr

LEFT JOIN CTE_MATERIAL_GROUP_DESC tg
    ON tg.matkl = ma.matkl

LEFT JOIN CTE_MATERIAL_TYPE_DESC tt
    ON tt.mtart = ma.mtart

LEFT JOIN CTE_PLANT_DATA pl
    ON pl.matnr = ma.matnr

LEFT JOIN CTE_VALUATION va
    ON va.matnr = ma.matnr

LEFT JOIN CTE_STORAGE st
    ON st.matnr = ma.matnr

ORDER BY
    pg.prgrp  ASC,
    ma.matnr  ASC
--this query is associated with the python script : sap_material_master_fixed.py

WITH mvke_dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY MATNR
            ORDER BY
                (
                    CASE WHEN MVGR1 IS NOT NULL AND MVGR1 <> '' THEN 1 ELSE 0 END
                  + CASE WHEN MVGR2 IS NOT NULL AND MVGR2 <> '' THEN 1 ELSE 0 END
                  + CASE WHEN MVGR3 IS NOT NULL AND MVGR3 <> '' THEN 1 ELSE 0 END
                  + CASE WHEN MVGR4 IS NOT NULL AND MVGR4 <> '' THEN 1 ELSE 0 END
                  + CASE WHEN MVGR5 IS NOT NULL AND MVGR5 <> '' THEN 1 ELSE 0 END
                ) DESC
        ) AS rn
    FROM sap_stg_hana."stg_sap_MM_mvke"
),
mvke_final AS (
    SELECT *
    FROM mvke_dedup
    WHERE rn = 1
)

SELECT
    -- =====================================================
    -- 1. COMPANY / ORGANIZATIONAL FIELDS
    -- =====================================================
    t001k.BUKRS AS COMPANY_CODE,
    marc.WERKS  AS BRANCH_ID,
    t001.WAERS  AS CURRENCY,

    -- =====================================================
    -- 2. MATERIAL CODE / DESCRIPTION
    -- =====================================================
    mara.MATNR  AS MATERIAL_CODE,
    makt.MAKTX  AS PRODUCT,
    mara.SIZE1  AS "SIZE",

    mara.MTART  AS MATERIAL_TYPE,
    t134t.MTBEZ AS MATERIAL_TYPE_DESC,

    mara.MATKL  AS MATERIAL_GROUP,
    t023t.WGBEZ AS MATERIAL_GROUP_DESC,

    -- =====================================================
    -- 3. MATERIAL HIERARCHY (LEVELS 1-5)
    -- =====================================================
    mvke.MVGR1  AS LEVEL1,
    tvm1t.BEZEI AS LEVEL1_DESC,

    mvke.MVGR2  AS LEVEL2,
    tvm2t.BEZEI AS LEVEL2_DESC,

    mvke.MVGR3  AS LEVEL3,
    tvm3t.BEZEI AS LEVEL3_DESC,

    mvke.MVGR4  AS LEVEL4,
    tvm4t.BEZEI AS LEVEL4_DESC,

    mvke.MVGR5  AS LEVEL5,
    tvm5t.BEZEI AS LEVEL5_DESC,

    -- =====================================================
    -- 4. MATERIAL ATTRIBUTES / UNITS / WEIGHT
    -- =====================================================
    mara.MEINS  AS UNIT_CODE,
    t006a.MSEHL AS UNIT,

    mara.GEWEI  AS WEIGHT_UNIT,
    mara.NTGEW  AS WEIGHT_PCS,

    mara.ERSDA    AS CREATION_DATE,
    mara.BRAND_ID AS BRAND_CODE,

    -- =====================================================
    -- 5. MRP / STOCK PARAMETERS
    -- =====================================================
    marc.MINBE AS REORDER_LEVEL,
    marc.MINBE AS MIN_LEVEL,
    marc.MABST AS MAX_LEVEL,
    marc.EISBE AS SAFETY_STOCK,
    marc.EISLO AS MINIMUM_SAFETY_STOCK,
    marc.WZEIT AS LEAD_TIME_IN_DAYS,
    marc.MAABC AS ABC,

    -- =====================================================
    -- 6. VALUATION / PRICING
    -- =====================================================
    mbew.VPRSV AS PRICE_CONTROL,
    mbew.STPRS AS STANDARD_PRICE,
    mbew.VERPR AS MOVING_AVG_PRICE,
    mbew.PEINH AS PRICE_UNIT,

    CASE
        WHEN mbew.VPRSV = 'V' THEN mbew.VERPR
        ELSE mbew.STPRS
    END AS PRICE,

    ROUND(
        (
            CASE
                WHEN mbew.VPRSV = 'V'
                    THEN mbew.VERPR::numeric
                ELSE mbew.STPRS::numeric
            END
        ) / NULLIF(mbew.PEINH::numeric, 0),
        4
    ) AS UNIT_PRICE_RAW,

    COALESCE(tcurx.CURRDEC::int, 2) AS CURRDEC_APPLIED,

    ROUND(
        (
            CASE
                WHEN mbew.VPRSV = 'V'
                    THEN mbew.VERPR::numeric
                ELSE mbew.STPRS::numeric
            END
        )
        / NULLIF(mbew.PEINH::numeric, 0)
        * POWER(
            10,
            2 - COALESCE(tcurx.CURRDEC::int, 2)
        )::numeric,
        2
    ) AS UNIT_PRICE_ADJUSTED

FROM sap_stg_hana."stg_sap_MM_mara" mara

-- =====================================================
-- MATERIAL / PLANT
-- =====================================================
INNER JOIN sap_stg_hana."stg_sap_MM_marc" marc
    ON mara.MATNR = marc.MATNR

-- =====================================================
-- TEXT TABLES (ENGLISH: SPRAS = 'E')
-- =====================================================

-- Material descriptions
LEFT JOIN sap_stg_hana."stg_sap_MM_makt" makt
    ON mara.MATNR = makt.MATNR
   AND makt.SPRAS = 'E'

-- Unit descriptions
LEFT JOIN sap_stg_hana."stg_sap_MM_t006a" t006a
    ON mara.MEINS = t006a.MSEHI
   AND t006a.SPRAS = 'E'

-- Material type descriptions
LEFT JOIN sap_stg_hana."stg_sap_MM_t134t" t134t
    ON mara.MTART = t134t.MTART
   AND t134t.SPRAS = 'E'

-- Material group descriptions
LEFT JOIN sap_stg_hana."stg_sap_MM_t023t" t023t
    ON mara.MATKL = t023t.MATKL
   AND t023t.SPRAS = 'E'

-- =====================================================
-- PLANT / VALUATION AREA / COMPANY
-- =====================================================

LEFT JOIN sap_stg_hana."stg_sap_MM_t001w" t001w
    ON marc.WERKS = t001w.WERKS

LEFT JOIN sap_stg_hana."stg_sap_MM_t001k" t001k
    ON t001w.BWKEY = t001k.BWKEY

LEFT JOIN sap_stg_hana."stg_sap_MM_t001" t001
    ON t001k.BUKRS = t001.BUKRS

-- =====================================================
-- SALES HIERARCHY (DEDUPLICATED MVKE)
-- =====================================================

LEFT JOIN mvke_final mvke
    ON mara.MATNR = mvke.MATNR

LEFT JOIN sap_stg_hana."stg_sap_MM_tvm1t" tvm1t
    ON mvke.MVGR1 = tvm1t.MVGR1
   AND tvm1t.SPRAS = 'E'

LEFT JOIN sap_stg_hana."stg_sap_MM_tvm2t" tvm2t
    ON mvke.MVGR2 = tvm2t.MVGR2
   AND tvm2t.SPRAS = 'E'

LEFT JOIN sap_stg_hana."stg_sap_MM_tvm3t" tvm3t
    ON mvke.MVGR3 = tvm3t.MVGR3
   AND tvm3t.SPRAS = 'E'

LEFT JOIN sap_stg_hana."stg_sap_MM_tvm4t" tvm4t
    ON mvke.MVGR4 = tvm4t.MVGR4
   AND tvm4t.SPRAS = 'E'

LEFT JOIN sap_stg_hana."stg_sap_MM_tvm5t" tvm5t
    ON mvke.MVGR5 = tvm5t.MVGR5
   AND tvm5t.SPRAS = 'E'

-- =====================================================
-- MATERIAL VALUATION / PRICE
-- =====================================================

LEFT JOIN sap_stg_hana."stg_sap_MM_mbew" mbew
    ON mara.MATNR = mbew.MATNR
   AND t001w.BWKEY = mbew.BWKEY
   AND COALESCE(mbew.BWTAR, '') = ''

-- =====================================================
-- CURRENCY DECIMAL PLACES
-- =====================================================

LEFT JOIN sap_stg_hana."stg_sap_MM_tcurx" tcurx
    ON t001.WAERS = tcurx.CURRKEY

-- =====================================================
-- FILTERS
-- =====================================================

WHERE marc.WERKS NOT IN ('RFST', 'RFDC');

-- Alternative:
-- WHERE marc.WERKS = '1102';
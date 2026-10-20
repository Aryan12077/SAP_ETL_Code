/* ====================================================================
   ZMM_MATERIAL_AGE -> PostgreSQL replica (ABAP-faithful), plant-wise

   Sources:
     stg_sap_MM_mbew
     stg_sap_MM_mbewh
     stg_sap_MM_mara
     stg_sap_MM_makt

   Changes:
     1. key_date = CURRENT_DATE
     2. Plants dynamically fetched from MBEW staging using BWKEY
     3. Processes all available plants
     4. Historical stock is matched to the corresponding plant
     5. Current Inventory Date added as the first output column
   ==================================================================== */

WITH params AS (
    SELECT
        CURRENT_DATE AS key_date,
        TRIM(bwkey) AS plant,
        100::NUMERIC AS price_scale
    FROM "sap_stg_hana"."stg_sap_MM_mbew"
    WHERE NULLIF(TRIM(bwkey), '') IS NOT NULL
    GROUP BY TRIM(bwkey)
),

kp AS (
    SELECT
        plant,
        price_scale,
        key_date,
        EXTRACT(YEAR FROM key_date)::INT AS key_year,
        (
            DATE_TRUNC('month', key_date)
            + INTERVAL '1 month'
        )::DATE - 1 AS p_date
    FROM params
),

/* Live stock: MBEW */
mbew AS (
    SELECT
        TRIM(b.matnr) AS matnr,
        TRIM(b.bwkey) AS bwkey,
        COALESCE(TRIM(b.bwtar), '') AS bwtar,

        CASE
            WHEN TRIM(b.lbkum) LIKE '%-'
                THEN -(NULLIF(REPLACE(TRIM(b.lbkum), '-', ''), '')::NUMERIC)
            ELSE NULLIF(TRIM(b.lbkum), '')::NUMERIC
        END AS qty,

        CASE
            WHEN TRIM(b.verpr) LIKE '%-'
                THEN -(NULLIF(REPLACE(TRIM(b.verpr), '-', ''), '')::NUMERIC)
            ELSE NULLIF(TRIM(b.verpr), '')::NUMERIC
        END AS verpr,

        CASE
            WHEN TRIM(b.salk3) LIKE '%-'
                THEN -(NULLIF(REPLACE(TRIM(b.salk3), '-', ''), '')::NUMERIC)
            ELSE NULLIF(TRIM(b.salk3), '')::NUMERIC
        END AS salk3,

        TRIM(b.lfgja)::INT AS lfgja,
        TRIM(b.lfmon)::INT AS lfmon,

        (
            MAKE_DATE(
                TRIM(b.lfgja)::INT,
                TRIM(b.lfmon)::INT,
                1
            ) + INTERVAL '1 month'
        )::DATE - 1 AS period_end

    FROM "sap_stg_hana"."stg_sap_MM_mbew" b

    JOIN kp
        ON TRIM(b.bwkey) = kp.plant

    WHERE NULLIF(TRIM(b.lfgja), '') IS NOT NULL
      AND NULLIF(TRIM(b.lfmon), '') IS NOT NULL
      AND TRIM(b.lfgja) <> '0000'
      AND TRIM(b.lfmon) NOT IN ('00', '0')
      AND TRIM(b.lfgja)::INT BETWEEN kp.key_year - 20 AND kp.key_year
),

mbew_nz AS (
    SELECT *
    FROM mbew
    WHERE qty <> 0
),

/* Historical stock: MBEWH */
mbewh AS (
    SELECT
        TRIM(h.matnr) AS matnr,
        TRIM(h.bwkey) AS bwkey,
        COALESCE(TRIM(h.bwtar), '') AS bwtar,

        CASE
            WHEN TRIM(h.lbkum) LIKE '%-'
                THEN -(NULLIF(REPLACE(TRIM(h.lbkum), '-', ''), '')::NUMERIC)
            ELSE NULLIF(TRIM(h.lbkum), '')::NUMERIC
        END AS qty,

        TRIM(h.lfgja)::INT AS lfgja,
        TRIM(h.lfmon)::INT AS lfmon,

        (
            MAKE_DATE(
                TRIM(h.lfgja)::INT,
                TRIM(h.lfmon)::INT,
                1
            ) + INTERVAL '1 month'
        )::DATE - 1 AS period_end

    FROM "sap_stg_hana"."stg_sap_MM_mbewh" h

    JOIN kp
        ON TRIM(h.bwkey) = kp.plant

    WHERE NULLIF(TRIM(h.lfgja), '') IS NOT NULL
      AND NULLIF(TRIM(h.lfmon), '') IS NOT NULL
      AND TRIM(h.lfgja) <> '0000'
      AND TRIM(h.lfmon) NOT IN ('00', '0')
      AND TRIM(h.lfgja)::INT BETWEEN kp.key_year - 23 AND kp.key_year
),

/* Match current stock with historical stock */
hist AS (
    SELECT
        m.matnr,
        m.bwkey,
        m.bwtar,
        m.qty AS q_cur,
        m.period_end AS cur_pe,
        h.qty AS h_qty,
        h.period_end AS h_pe,

        ROW_NUMBER() OVER (
            PARTITION BY m.matnr, m.bwkey, m.bwtar
            ORDER BY h.lfgja DESC, h.lfmon DESC
        ) AS rn

    FROM mbew_nz m

    JOIN mbewh h
        ON h.matnr = m.matnr
       AND h.bwkey = m.bwkey
       AND h.bwtar = m.bwtar
),

/* Determine the newer snapshot quantity and date */
steps AS (
    SELECT
        *,

        CASE
            WHEN rn = 1 THEN q_cur
            ELSE LAG(h_qty) OVER w
        END AS n_qty,

        CASE
            WHEN rn = 1 THEN cur_pe
            ELSE LAG(h_pe) OVER w
        END AS n_pe

    FROM hist

    WINDOW w AS (
        PARTITION BY matnr, bwkey, bwtar
        ORDER BY rn
    )
),

flagged AS (
    SELECT
        *,
        (h_qty <> 0 AND n_qty > h_qty) AS ok
    FROM steps
),

walk AS (
    SELECT
        *,

        COUNT(*) FILTER (
            WHERE NOT ok
        ) OVER (
            PARTITION BY matnr, bwkey, bwtar
            ORDER BY rn
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prior_fails

    FROM flagged
),

/* Build inventory-ageing layers */
layers AS (
    SELECT
        matnr,
        bwkey,
        bwtar,
        n_pe AS layer_pe,

        CASE
            WHEN ok THEN n_qty - h_qty
            ELSE n_qty
        END AS layer_qty,

        FALSE AS no_hist

    FROM walk
    WHERE prior_fails = 0

    UNION ALL

    SELECT
        m.matnr,
        m.bwkey,
        m.bwtar,
        m.period_end AS layer_pe,
        m.qty AS layer_qty,
        TRUE AS no_hist

    FROM mbew_nz m

    WHERE NOT EXISTS (
        SELECT 1
        FROM mbewh h
        WHERE h.matnr = m.matnr
          AND h.bwkey = m.bwkey
          AND h.bwtar = m.bwtar
    )
),

/* Calculate age relative to month-end */
bucketed AS (
    SELECT
        l.matnr,
        l.bwkey,
        l.bwtar,
        l.layer_qty,
        l.no_hist,
        kp.p_date - l.layer_pe AS d

    FROM layers l

    JOIN kp
        ON kp.plant = l.bwkey
),

/* Aggregate inventory into ageing buckets */
agg AS (
    SELECT
        matnr,
        bwkey,
        bwtar,

        SUM(layer_qty) AS allocated_qty,

        SUM(layer_qty) FILTER (
            WHERE d BETWEEN 0 AND 30
        ) AS day_0_30,

        SUM(layer_qty) FILTER (
            WHERE d BETWEEN 31 AND 60
        ) AS day_31_60,

        SUM(layer_qty) FILTER (
            WHERE d BETWEEN 61 AND 90
        ) AS day_61_90,

        SUM(layer_qty) FILTER (
            WHERE d BETWEEN 91 AND 120
        ) AS day_91_120,

        SUM(layer_qty) FILTER (
            WHERE d BETWEEN 121 AND 180
        ) AS day_121_180,

        SUM(layer_qty) FILTER (
            WHERE d BETWEEN 181 AND 270
        ) AS day_181_270,

        SUM(layer_qty) FILTER (
            WHERE d BETWEEN 271 AND 365
        ) AS day_271_365,

        SUM(layer_qty) FILTER (
            WHERE d > 365
        ) AS day_m_365,

        BOOL_OR(no_hist) AS no_history_flag

    FROM bucketed

    GROUP BY
        matnr,
        bwkey,
        bwtar
)

/* Final output */
SELECT
    CURRENT_DATE AS "Current Inventory Date",
    LTRIM(m.matnr, '0') AS "Material",
    mk.maktx AS "Material Desc.",
    ma.meins AS "UOM",
    ma.mtart AS "Material Type",
    m.bwkey AS "Plant",
    m.qty AS "Current Quantity",
    m.verpr * kp.price_scale AS "MM60 Price",

    COALESCE(a.day_0_30, 0) AS "00 - 30 Days",
    COALESCE(a.day_31_60, 0) AS "31 - 60 Days",
    COALESCE(a.day_61_90, 0) AS "61 - 90 Days",
    COALESCE(a.day_91_120, 0) AS "91 - 120 Days",
    COALESCE(a.day_121_180, 0) AS "121 - 180 Days",
    COALESCE(a.day_181_270, 0) AS "181 - 270 Days",
    COALESCE(a.day_271_365, 0) AS "271 - 365 Days",
    COALESCE(a.day_m_365, 0) AS "More than 365 Days",

    /* Diagnostic: quantity not allocated to the displayed buckets */
    m.qty
        - COALESCE(a.day_0_30, 0)
        - COALESCE(a.day_31_60, 0)
        - COALESCE(a.day_61_90, 0)
        - COALESCE(a.day_91_120, 0)
        - COALESCE(a.day_121_180, 0)
        - COALESCE(a.day_181_270, 0)
        - COALESCE(a.day_271_365, 0)
        - COALESCE(a.day_m_365, 0)
        AS "unallocated_qty",

    COALESCE(a.no_history_flag, FALSE) AS "no_history_flag",
    m.salk3 AS "stock_value_internal"

FROM mbew_nz m

JOIN kp
    ON kp.plant = m.bwkey

LEFT JOIN agg a
    ON a.matnr = m.matnr
   AND a.bwkey = m.bwkey
   AND a.bwtar = m.bwtar

LEFT JOIN "sap_stg_hana"."stg_sap_MM_mara" ma
    ON TRIM(ma.matnr) = m.matnr

LEFT JOIN "sap_stg_hana"."stg_sap_MM_makt" mk
    ON TRIM(mk.matnr) = m.matnr
   AND TRIM(mk.spras) = 'E'

ORDER BY
    m.matnr,
    m.bwtar;
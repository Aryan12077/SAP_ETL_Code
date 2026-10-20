/* ============================================================
    Financial Year Master — T001 + T009 + T009B | PostgreSQL
   ============================================================ */

WITH cte_years AS (
    -- Window of fiscal years to evaluate (current year -2 .. +1)
    SELECT y AS fy
    FROM generate_series(
        EXTRACT(YEAR FROM CURRENT_DATE)::int - 2,
        EXTRACT(YEAR FROM CURRENT_DATE)::int + 1
    ) AS y
),

cte_periods AS (
    -- Year-independent period definitions (T009B.BDATJ blank / 0000)
    SELECT
        TRIM(periv)                                     AS periv,
        CAST(NULLIF(TRIM(bumon), '') AS INTEGER)        AS bumon,
        CAST(NULLIF(TRIM(butag), '') AS INTEGER)        AS butag,
        -- year shift can arrive as '-1', '1-', '0' or blank
        CASE
            WHEN POSITION('-' IN reljr) > 0
                THEN -1 * COALESCE(CAST(NULLIF(REPLACE(TRIM(reljr), '-', ''), '') AS INTEGER), 0)
            ELSE COALESCE(CAST(NULLIF(TRIM(reljr), '') AS INTEGER), 0)
        END                                             AS year_shift
    FROM "sap_stg_hana"."stg_sap_financial_year_master_t009b"
    WHERE COALESCE(NULLIF(TRIM(bdatj), ''), '0000') = '0000'
      AND NULLIF(TRIM(bumon), '') IS NOT NULL
      AND NULLIF(TRIM(butag), '') IS NOT NULL
),

cte_period_dates AS (
    -- Calendar end date of every period for every fiscal year
    SELECT
        p.periv,
        y.fy,
        CASE
            WHEN p.bumon = 2 AND p.butag >= 28
                THEN x.month_end                                   -- Feb: always month end
            ELSE make_date(y.fy + p.year_shift, p.bumon,
                           LEAST(p.butag, EXTRACT(DAY FROM x.month_end)::int))
        END AS period_end
    FROM cte_periods p
    CROSS JOIN cte_years y
    CROSS JOIN LATERAL (
        SELECT (make_date(y.fy + p.year_shift, p.bumon, 1)
                + INTERVAL '1 month' - INTERVAL '1 day')::date AS month_end
    ) x
),

cte_fy_end AS (
    -- Fiscal year end = latest period end (special periods 13-16 share it)
    SELECT periv, fy, MAX(period_end) AS fy_end
    FROM cte_period_dates
    GROUP BY periv, fy
),

cte_fy_range AS (
    -- Start of a fiscal year = day after the previous year's end
    SELECT
        e.periv,
        e.fy,
        (p.fy_end + 1)  AS fy_start,
        e.fy_end
    FROM cte_fy_end e
    INNER JOIN cte_fy_end p
        ON p.periv = e.periv
       AND p.fy    = e.fy - 1

    UNION ALL

    -- Fallback: calendar-year variants with no T009B rows
    SELECT
        TRIM(t.periv),
        y.fy,
        make_date(y.fy, 1, 1),
        make_date(y.fy, 12, 31)
    FROM "sap_stg_hana"."stg_sap_financial_year_master_t009" t
    CROSS JOIN cte_years y
    WHERE t.xkale = 'X'
      AND NOT EXISTS (
          SELECT 1 FROM cte_periods p WHERE p.periv = TRIM(t.periv)
      )
)

SELECT
    TRIM(c.bukrs)                       AS "company_code",
    CAST(f.fy AS TEXT)                  AS "fiscal_year",
    f.fy_start                          AS "fy_start_date",
    f.fy_end                            AS "fy_end_date",
    TRIM(c.periv)                       AS "fiscal_variant"

FROM "sap_stg_hana"."stg_sap_financial_year_master_t001" c

INNER JOIN cte_fy_range f
    ON f.periv = TRIM(c.periv)

-- Current fiscal year only. Delete this line to return every year in the window.
WHERE CURRENT_DATE BETWEEN f.fy_start AND f.fy_end

ORDER BY
    c.bukrs,
    f.fy;
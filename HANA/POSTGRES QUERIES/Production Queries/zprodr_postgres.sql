/* =====================================================================
   ZPRODR - simple PostgreSQL query on the staging tables
   (sap_stg_hana.stg_sap_zprodr_<table>, columns are lower-case TEXT)

   The "tmg" CTE already lists the dynamic ZMM tables found in the extraction.
   Plant 1102 and dates 20260101-20260630 match the extraction run.
   ===================================================================== */

WITH p AS (
    SELECT '20260101'::text AS date_from,     -- S_BUDAT low
           '20260630'::text AS date_to        -- S_BUDAT high (= "To Date Prod." day)
),

/* ---------------------------------------------------------------------
   ABAP reads a table whose NAME is stored in ZCUSTOM_TAB.TMG_NAME.
   These are the dynamic tables that were staged by the extraction
   (columns that do not exist in a given table are NULL, like the
   TRY/CATCH in the ABAP).
   ZMM0008, ZMM0010, ZMM0012, ZMM0013, ZMM0016, ZMM0017, ZMM0018 have none of
   the expected columns in SAP, so they were not staged and give blank
   quality / shade (same as the ABAP).
   --------------------------------------------------------------------- */
tmg AS (
    SELECT 'ZMM0011'::text AS tmg_name,
           zqlty_sort, zqlty_sortd,
           zdesign_no, zdesign_nod,
           NULL::text AS zshade_no, NULL::text AS zshade_nod
    FROM   sap_stg_hana.stg_sap_zprodr_zmm0011
    UNION ALL
    SELECT 'ZMM0014',
           zqlty_sort, zqlty_sortd,
           NULL::text, NULL::text,
           zshade_no, zshade_nod
    FROM   sap_stg_hana.stg_sap_zprodr_zmm0014
    UNION ALL
    SELECT 'ZMM0015',
           zqlty_sort, zqlty_sortd,
           NULL::text, NULL::text,
           NULL::text, NULL::text
    FROM   sap_stg_hana.stg_sap_zprodr_zmm0015
    UNION ALL
    SELECT 'ZMM0019',
           zqlty_sort, zqlty_sortd,
           NULL::text, NULL::text,
           zshade_no, zshade_nod
    FROM   sap_stg_hana.stg_sap_zprodr_zmm0019
    UNION ALL
    SELECT 'ZMM0020',
           zqlty_sort, zqlty_sortd,
           NULL::text, NULL::text,
           zshade_no, zshade_nod
    FROM   sap_stg_hana.stg_sap_zprodr_zmm0020
    UNION ALL
    SELECT 'ZMM0021',
           zqlty_sort, zqlty_sortd,
           NULL::text, NULL::text,
           zshade_no, zshade_nod
    FROM   sap_stg_hana.stg_sap_zprodr_zmm0021
),

/* ----- the ABAP SELECT with all its joins ----- */
base AS (
    SELECT
        a.mblnr, a.mjahr, a.zeile,
        a.werks, a.matnr, a.budat,
        ROUND(                                          -- MENGE / 10  (3 decimals like QUAN)
            CASE WHEN NULLIF(TRIM(a.menge), '') IS NULL THEN 0
                 WHEN TRIM(a.menge) LIKE '%-' THEN -REPLACE(TRIM(a.menge), '-', '')::numeric
                 ELSE TRIM(a.menge)::numeric
            END / 10, 3)                                AS menge_div10,
        c.maktx, d.fevor, e.txt,
        f.zqlty_sort, f.zshade_no, f.zdesign_no, UPPER(TRIM(f.tmg_name)) AS tmg_name,
        g.mvgr1, g.mvgr2, g.mvgr3, g.mvgr4, g.mvgr5,
        t1.bezei AS bezei1, t2.bezei AS bezei2, t3.bezei AS bezei3,
        t4.bezei AS bezei4, t5.bezei AS bezei5
    FROM sap_stg_hana.stg_sap_zprodr_matdoc a
    CROSS JOIN p
    JOIN      sap_stg_hana.stg_sap_zprodr_afpo        b  ON b.aufnr = a.aufnr
    LEFT JOIN sap_stg_hana.stg_sap_zprodr_makt        c  ON c.matnr = a.matnr AND c.spras = 'E'
    LEFT JOIN sap_stg_hana.stg_sap_zprodr_marc        d  ON d.matnr = a.matnr AND d.werks = a.werks
    LEFT JOIN sap_stg_hana.stg_sap_zprodr_t024f       e  ON e.fevor = d.fevor AND e.werks = d.werks
    LEFT JOIN sap_stg_hana.stg_sap_zprodr_zcustom_tab f  ON f.matnr = a.matnr
    LEFT JOIN sap_stg_hana.stg_sap_zprodr_mvke        g  ON g.matnr = a.matnr AND g.vkorg = '1100' AND g.vtweg = '01'
    LEFT JOIN sap_stg_hana.stg_sap_zprodr_tvm1t       t1 ON t1.mvgr1 = g.mvgr1 AND t1.spras = 'E'
    LEFT JOIN sap_stg_hana.stg_sap_zprodr_tvm2t       t2 ON t2.mvgr2 = g.mvgr2 AND t2.spras = 'E'
    LEFT JOIN sap_stg_hana.stg_sap_zprodr_tvm3t       t3 ON t3.mvgr3 = g.mvgr3 AND t3.spras = 'E'
    LEFT JOIN sap_stg_hana.stg_sap_zprodr_tvm4t       t4 ON t4.mvgr4 = g.mvgr4 AND t4.spras = 'E'
    LEFT JOIN sap_stg_hana.stg_sap_zprodr_tvm5t       t5 ON t5.mvgr5 = g.mvgr5 AND t5.spras = 'E'
    WHERE a.werks IN ('1102')                           -- S_WERKS
      AND a.budat BETWEEN p.date_from AND p.date_to     -- S_BUDAT
      AND a.bwart = '101'
      AND COALESCE(a.cancelled, '') = ''
      -- AND a.matnr IN ('...')                         -- S_MATNR (optional)
      -- AND b.dauat IN ('...')                         -- S_DAUAT (optional)
      -- AND d.fevor IN ('...')                         -- S_FEVOR (optional)
)

/* ----- one row per WERKS + MATNR (first row gives the attributes, quantities are summed) ----- */
SELECT DISTINCT ON (b.werks, b.matnr)
    b.werks,
    LEFT(COALESCE((SELECT t.zqlty_sortd FROM tmg t
                   WHERE t.tmg_name = b.tmg_name AND t.zqlty_sort = b.zqlty_sort
                   LIMIT 1), ''), 20)                                   AS quality,
    CASE WHEN COALESCE(b.tmg_name, '') = '' THEN ''
         ELSE LEFT(
                COALESCE((SELECT t.zdesign_nod FROM tmg t
                          WHERE t.tmg_name = b.tmg_name AND t.zdesign_no = b.zdesign_no
                          LIMIT 1), '')
             || COALESCE((SELECT t.zshade_nod FROM tmg t
                          WHERE t.tmg_name = b.tmg_name AND t.zshade_no = b.zshade_no
                          LIMIT 1), ''), 60)
    END                                                                 AS shade,
    b.matnr,
    COALESCE(b.maktx, '')   AS maktx,
    COALESCE(b.mvgr1, '')   AS mvgr1,  COALESCE(b.bezei1, '') AS bezei1,
    COALESCE(b.mvgr2, '')   AS mvgr2,  COALESCE(b.bezei2, '') AS bezei2,
    COALESCE(b.mvgr3, '')   AS mvgr3,  COALESCE(b.bezei3, '') AS bezei3,
    COALESCE(b.mvgr4, '')   AS mvgr4,  COALESCE(b.bezei4, '') AS bezei4,
    COALESCE(b.mvgr5, '')   AS mvgr5,  COALESCE(b.bezei5, '') AS bezei5,
    SUM(b.menge_div10) OVER (PARTITION BY b.werks, b.matnr)::numeric(15,3)        AS from_to_prod,
    SUM(CASE WHEN b.budat = p.date_to THEN b.menge_div10 ELSE 0 END)
        OVER (PARTITION BY b.werks, b.matnr)::numeric(15,3)                       AS to_prod,
    COALESCE(m.meins, '')   AS meins,
    COALESCE(b.fevor, '')   AS fevor,
    COALESCE(b.txt, '')     AS txt
FROM base b
CROSS JOIN p
LEFT JOIN sap_stg_hana.stg_sap_zprodr_mara m ON m.matnr = b.matnr
ORDER BY b.werks, b.matnr, b.mblnr, b.mjahr, b.zeile;
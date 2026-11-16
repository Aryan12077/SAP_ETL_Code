WITH params AS (                              -- == selection screen ==
    SELECT DATE '2026-01-01' AS date_from,   -- S_BUDAT-LOW
           DATE '2026-06-30' AS date_to,     -- S_BUDAT-HIGH (also "To Date Prod." day)
           NULL::text[]      AS fevor_list   -- S_FEVOR, e.g. ARRAY['001','002']; NULL = all
),
p AS (
    SELECT *, to_char(date_from, 'YYYYMMDD') AS d_from, to_char(date_to, 'YYYYMMDD') AS d_to
    FROM params
),
tmg_src AS (                                  -- dynamic ZCUSTOM_TAB.TMG_NAME tables
    SELECT NULL::text AS tmg_name, NULL::text AS lookup_type, NULL::text AS code, NULL::text AS descr WHERE false
    /* add one block per TMG table, e.g. for TMG_NAME = 'ZTMG_A':
    UNION ALL SELECT 'ZTMG_A', 'QLTY',   zqlty_sort, zqlty_sortd FROM sap_stg_hana.stg_sap_zprodr_ztmg_a
    UNION ALL SELECT 'ZTMG_A', 'DESIGN', zdesign_no, zdesign_nod FROM sap_stg_hana.stg_sap_zprodr_ztmg_a
    UNION ALL SELECT 'ZTMG_A', 'SHADE',  zshade_no,  zshade_nod  FROM sap_stg_hana.stg_sap_zprodr_ztmg_a  */
),
tmg AS (                                      -- SELECT SINGLE => one row per code
    SELECT DISTINCT ON (tmg_name, lookup_type, code) tmg_name, lookup_type, code, descr
    FROM   tmg_src
    ORDER  BY tmg_name, lookup_type, code, descr
),
i_matdoc AS (
    SELECT a.werks, a.matnr, a.budat,
           round(COALESCE(NULLIF(regexp_replace(btrim(a.menge), '^(.*)-$', '-\1'), '')::numeric, 0) / 10, 3) AS qty,
           coalesce(c.maktx, '')           AS maktx,
           coalesce(d.fevor, '')           AS fevor,
           coalesce(e.txt, '')             AS txt,
           coalesce(btrim(f.tmg_name), '') AS tmg_name,
           coalesce(f.zqlty_sort, '')      AS zqlty_sort,
           coalesce(f.zdesign_no, '')      AS zdesign_no,
           coalesce(f.zshade_no, '')       AS zshade_no,
           coalesce(g.mvgr1, '') AS mvgr1, coalesce(t1.bezei, '') AS bezei1,
           coalesce(g.mvgr2, '') AS mvgr2, coalesce(t2.bezei, '') AS bezei2,
           coalesce(g.mvgr3, '') AS mvgr3, coalesce(t3.bezei, '') AS bezei3,
           coalesce(g.mvgr4, '') AS mvgr4, coalesce(t4.bezei, '') AS bezei4,
           coalesce(g.mvgr5, '') AS mvgr5, coalesce(t5.bezei, '') AS bezei5,
           p.d_to
    FROM   sap_stg_hana.stg_sap_zprodr_matdoc a
    CROSS  JOIN p
    JOIN   sap_stg_hana.stg_sap_zprodr_afpo             b  ON b.aufnr = a.aufnr
    LEFT   JOIN sap_stg_hana.stg_sap_zprodr_makt        c  ON c.matnr = a.matnr AND c.spras = 'E'
    LEFT   JOIN sap_stg_hana.stg_sap_zprodr_marc        d  ON d.matnr = a.matnr AND d.werks = a.werks
    LEFT   JOIN sap_stg_hana.stg_sap_zprodr_t024f       e  ON e.fevor = d.fevor AND e.werks = d.werks
    LEFT   JOIN sap_stg_hana.stg_sap_zprodr_zcustom_tab f  ON f.matnr = a.matnr
    LEFT   JOIN sap_stg_hana.stg_sap_zprodr_mvke        g  ON g.matnr = a.matnr AND g.vkorg = '1100' AND g.vtweg = '01'
    LEFT   JOIN sap_stg_hana.stg_sap_zprodr_tvm1t       t1 ON t1.mvgr1 = g.mvgr1 AND t1.spras = 'E'
    LEFT   JOIN sap_stg_hana.stg_sap_zprodr_tvm2t       t2 ON t2.mvgr2 = g.mvgr2 AND t2.spras = 'E'
    LEFT   JOIN sap_stg_hana.stg_sap_zprodr_tvm3t       t3 ON t3.mvgr3 = g.mvgr3 AND t3.spras = 'E'
    LEFT   JOIN sap_stg_hana.stg_sap_zprodr_tvm4t       t4 ON t4.mvgr4 = g.mvgr4 AND t4.spras = 'E'
    LEFT   JOIN sap_stg_hana.stg_sap_zprodr_tvm5t       t5 ON t5.mvgr5 = g.mvgr5 AND t5.spras = 'E'
    WHERE  a.bwart = '101'
      AND  a.cancelled = ''
      AND  a.budat BETWEEN p.d_from AND p.d_to
      AND  (p.fevor_list IS NULL OR d.fevor = ANY (p.fevor_list))
),
row_level AS (
    SELECT m.*,
           CASE WHEN m.tmg_name = '' THEN '' ELSE coalesce(q.descr, '') END AS quality,
           CASE WHEN m.tmg_name = '' THEN ''
                ELSE coalesce(dz.descr, '') || coalesce(sh.descr, '') END   AS shade
    FROM   i_matdoc m
    LEFT   JOIN tmg q  ON q.tmg_name  = m.tmg_name AND q.lookup_type  = 'QLTY'   AND q.code  = m.zqlty_sort
    LEFT   JOIN tmg dz ON dz.tmg_name = m.tmg_name AND dz.lookup_type = 'DESIGN' AND dz.code = m.zdesign_no
    LEFT   JOIN tmg sh ON sh.tmg_name = m.tmg_name AND sh.lookup_type = 'SHADE'  AND sh.code = m.zshade_no
)
SELECT r.werks                                                 AS "Plant",
       min(r.quality)                                          AS "Quality",
       min(r.shade)                                            AS "Shade",
       r.matnr                                                 AS "Material",
       min(r.maktx)                                            AS "Description",
       min(r.mvgr1)                                            AS "Product Hierarchy 1",
       min(r.bezei1)                                           AS "Product Hierarchy 1 desc.",
       min(r.mvgr2)                                            AS "Product Hierarchy 2",
       min(r.bezei2)                                           AS "Product Hierarchy 2 desc.",
       min(r.mvgr3)                                            AS "Product Hierarchy 3",
       min(r.bezei3)                                           AS "Product Hierarchy 3 desc",
       min(r.mvgr4)                                            AS "Product Hierarchy 4",
       min(r.bezei4)                                           AS "Product Hierarchy 4 desc",
       min(r.mvgr5)                                            AS "Product Hierarchy 5",
       min(r.bezei5)                                           AS "Product Hierarchy 5 desc",
       sum(r.qty)                                              AS "From to To Prod.",
       coalesce(sum(r.qty) FILTER (WHERE r.budat = r.d_to), 0) AS "To Date Prod.",
       coalesce(min(mr.meins), '')                             AS "UOM",
       min(r.fevor)                                            AS "Division",
       min(r.txt)                                              AS "Division Desc."
FROM   row_level r
LEFT   JOIN sap_stg_hana.stg_sap_zprodr_mara mr ON mr.matnr = r.matnr
GROUP  BY r.werks, r.matnr
ORDER  BY r.werks, r.matnr;
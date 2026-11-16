WITH params AS (
    SELECT DATE '2026-01-01' AS date_from,   -- S_BUDAT-LOW
           DATE '2026-06-30' AS date_to,     -- S_BUDAT-HIGH
           NULL::text[] AS machine_list,     -- S_MACH
           NULL::text[] AS arbpl_list,       -- S_ARBPL
           NULL::text[] AS section_list,     -- S_SECT (section code)
           NULL::text[] AS dauat_list        -- S_DAUAT
),
p AS (
    SELECT *, to_char(date_from, 'YYYYMMDD') AS d_from, to_char(date_to, 'YYYYMMDD') AS d_to,
           (date_to - date_from + 1) AS days
    FROM params
),
sect_txt AS (
    SELECT DISTINCT ON (domvalue_l) domvalue_l, ddtext
    FROM   sap_stg_hana.stg_sap_zmprodr_dd07t
    WHERE  domname = 'ZSACTIONNAME' AND ddlanguage = 'E'
    ORDER  BY domvalue_l, valpos
),
i_data AS (                                   -- machines + looms
    SELECT 1 AS src, a.werks, a.arbpl, a.machine, a.ktext, ''::text AS sectioncode, ''::text AS sectionname
    FROM   sap_stg_hana.stg_sap_zmprodr_zmachineno a
    UNION ALL
    SELECT 2, z.werks, '', z.loomno, '', z.sectionname, coalesce(t.ddtext, '')
    FROM   sap_stg_hana.stg_sap_zmprodr_zloom z
    LEFT   JOIN sect_txt t ON t.domvalue_l = z.sectionname
),
i_afru AS (
    SELECT a.arbpl, a.machine, ''::text AS sectionname,
           COALESCE(NULLIF(regexp_replace(btrim(a.std_prod), '^(.*)-$', '-\1'), '')::numeric, 0) AS std_prod,
           COALESCE(NULLIF(regexp_replace(btrim(b.gmnga), '^(.*)-$', '-\1'), '')::numeric, 0)    AS gmnga,
           b.gmein
    FROM   sap_stg_hana.stg_sap_zmprodr_zmachineno a
    CROSS  JOIN p
    JOIN   sap_stg_hana.stg_sap_zmprodr_afru b ON b.zzmachine = a.machine AND b.werks = a.werks
    LEFT   JOIN sap_stg_hana.stg_sap_zmprodr_afpo c ON c.aufnr = b.aufnr
    WHERE  b.budat BETWEEN p.d_from AND p.d_to
      AND  b.stokz = '' AND b.stzhl = ''
      AND  (p.dauat_list IS NULL OR c.dauat = ANY (p.dauat_list))
    UNION ALL
    SELECT '', z.loomno, coalesce(t.ddtext, ''),
           COALESCE(NULLIF(regexp_replace(btrim(z.std_prod), '^(.*)-$', '-\1'), '')::numeric, 0),
           COALESCE(NULLIF(regexp_replace(btrim(b.gmnga), '^(.*)-$', '-\1'), '')::numeric, 0),
           b.gmein
    FROM   sap_stg_hana.stg_sap_zmprodr_zloom z
    CROSS  JOIN p
    JOIN   sap_stg_hana.stg_sap_zmprodr_afru b ON b.zzmachine = z.sectionname || z.loomno AND b.werks = z.werks
    LEFT   JOIN sap_stg_hana.stg_sap_zmprodr_afpo c ON c.aufnr = b.aufnr
    LEFT   JOIN sect_txt t ON t.domvalue_l = z.sectionname
    WHERE  b.budat BETWEEN p.d_from AND p.d_to
      AND  b.stokz = '' AND b.stzhl = ''
      AND  (p.dauat_list IS NULL OR c.dauat = ANY (p.dauat_list))
),
afru_grp AS (
    SELECT arbpl, machine, sectionname,
           sum(gmnga) AS gmnga, min(gmein) AS gmein, min(std_prod) AS std_prod
    FROM   i_afru
    GROUP  BY arbpl, machine, sectionname
),
final AS (
    SELECT d.src, d.werks, d.arbpl, d.machine, d.ktext, d.sectioncode, d.sectionname,
           coalesce(g.std_prod, 0) * p.days AS std_prod_total,
           coalesce(g.gmein, '')            AS gmein,
           coalesce(g.gmnga, 0)             AS gmnga_raw,
           p.date_from, p.date_to
    FROM   i_data d
    CROSS  JOIN p
    LEFT   JOIN afru_grp g ON g.arbpl = d.arbpl AND g.machine = d.machine AND g.sectionname = d.sectionname
    WHERE  (p.machine_list IS NULL OR d.machine     = ANY (p.machine_list))
      AND  (p.arbpl_list   IS NULL OR d.arbpl       = ANY (p.arbpl_list))
      AND  (p.section_list IS NULL OR d.sectioncode = ANY (p.section_list))
)
SELECT werks                    AS "Plant",
       date_from                AS "From Date",
       date_to                  AS "To Date",
       arbpl                    AS "Work Center",
       machine                  AS "Machine No.",
       ktext                    AS "MCN Description",
       std_prod_total           AS "Std. Production",
       gmein                    AS "Uom",
       round(gmnga_raw / 10, 3) AS "Act. Production",
       CASE WHEN std_prod_total <> 0
            THEN round(gmnga_raw / std_prod_total * 100, 3) ELSE 0 END AS "Achieve %",
       sectionname              AS "Section"
FROM   final
ORDER  BY src, arbpl, machine, sectioncode;

--query 2 : detailed summary

WITH params AS (
    SELECT DATE '2026-01-01' AS date_from,
           DATE '2026-06-30' AS date_to,
           NULL::text[] AS machine_list,     -- S_MACH
           NULL::text[] AS arbpl_list,       -- S_ARBPL
           NULL::text[] AS section_list,     -- S_SECT
           NULL::text[] AS dauat_list,       -- S_DAUAT
           NULL::text[] AS matnr_list        -- S_MATNR
),
p AS (
    SELECT *, to_char(date_from, 'YYYYMMDD') AS d_from, to_char(date_to, 'YYYYMMDD') AS d_to,
           (date_to - date_from + 1) AS days
    FROM params
),
sect_txt AS (
    SELECT DISTINCT ON (domvalue_l) domvalue_l, ddtext
    FROM   sap_stg_hana.stg_sap_zmprodr_dd07t
    WHERE  domname = 'ZSACTIONNAME' AND ddlanguage = 'E'
    ORDER  BY domvalue_l, valpos
),
tmg_src AS (                                  -- dynamic ZCUSTOM_TAB.TMG_NAME tables
    SELECT NULL::text AS tmg_name, NULL::text AS lookup_type, NULL::text AS code, NULL::text AS descr WHERE false
    /* UNION ALL SELECT 'ZTMG_A', 'QLTY',   zqlty_sort, zqlty_sortd FROM sap_stg_hana.stg_sap_zmprodr_ztmg_a
       UNION ALL SELECT 'ZTMG_A', 'DESIGN', zdesign_no, zdesign_nod FROM sap_stg_hana.stg_sap_zmprodr_ztmg_a
       UNION ALL SELECT 'ZTMG_A', 'SHADE',  zshade_no,  zshade_nod  FROM sap_stg_hana.stg_sap_zmprodr_ztmg_a  */
),
tmg AS (
    SELECT DISTINCT ON (tmg_name, lookup_type, code) tmg_name, lookup_type, code, descr
    FROM   tmg_src
    ORDER  BY tmg_name, lookup_type, code, descr
),
i_data AS (
    SELECT 1 AS src, a.werks, a.arbpl, a.machine, a.ktext, ''::text AS sectioncode, ''::text AS sectionname
    FROM   sap_stg_hana.stg_sap_zmprodr_zmachineno a
    UNION ALL
    SELECT 2, z.werks, '', z.loomno, '', z.sectionname, coalesce(t.ddtext, '')
    FROM   sap_stg_hana.stg_sap_zmprodr_zloom z
    LEFT   JOIN sect_txt t ON t.domvalue_l = z.sectionname
),
afru_base AS (
    SELECT a.werks, a.arbpl, a.machine, ''::text AS sectionname, a.std_prod,
           b.rueck, b.rmzhl, b.aufnr, b.gmnga, b.gmein
    FROM   sap_stg_hana.stg_sap_zmprodr_zmachineno a
    CROSS  JOIN p
    JOIN   sap_stg_hana.stg_sap_zmprodr_afru b ON b.zzmachine = a.machine AND b.werks = a.werks
    WHERE  b.budat BETWEEN p.d_from AND p.d_to AND b.stokz = '' AND b.stzhl = ''
    UNION ALL
    SELECT z.werks, '', z.loomno, coalesce(t.ddtext, ''), z.std_prod,
           b.rueck, b.rmzhl, b.aufnr, b.gmnga, b.gmein
    FROM   sap_stg_hana.stg_sap_zmprodr_zloom z
    CROSS  JOIN p
    JOIN   sap_stg_hana.stg_sap_zmprodr_afru b ON b.zzmachine = z.sectionname || z.loomno AND b.werks = z.werks
    LEFT   JOIN sect_txt t ON t.domvalue_l = z.sectionname
    WHERE  b.budat BETWEEN p.d_from AND p.d_to AND b.stokz = '' AND b.stzhl = ''
),
i_afru AS (
    SELECT x.werks, x.arbpl, x.machine, x.sectionname, x.rueck, x.rmzhl,
           COALESCE(NULLIF(regexp_replace(btrim(x.std_prod), '^(.*)-$', '-\1'), '')::numeric, 0) AS std_prod,
           COALESCE(NULLIF(regexp_replace(btrim(x.gmnga), '^(.*)-$', '-\1'), '')::numeric, 0)    AS gmnga,
           x.gmein,
           coalesce(c.matnr, '')           AS matnr,
           coalesce(d.maktx, '')           AS maktx,
           coalesce(btrim(f.tmg_name), '') AS tmg_name,
           coalesce(f.zqlty_sort, '')      AS zqlty_sort,
           coalesce(f.zdesign_no, '')      AS zdesign_no,
           coalesce(f.zshade_no, '')       AS zshade_no,
           coalesce(g.mvgr1, '') AS mvgr1, coalesce(t1.bezei, '') AS bezei1,
           coalesce(g.mvgr2, '') AS mvgr2, coalesce(t2.bezei, '') AS bezei2,
           coalesce(g.mvgr3, '') AS mvgr3, coalesce(t3.bezei, '') AS bezei3,
           coalesce(g.mvgr4, '') AS mvgr4, coalesce(t4.bezei, '') AS bezei4,
           coalesce(g.mvgr5, '') AS mvgr5, coalesce(t5.bezei, '') AS bezei5
    FROM   afru_base x
    CROSS  JOIN p
    LEFT   JOIN sap_stg_hana.stg_sap_zmprodr_afpo        c  ON c.aufnr = x.aufnr
    LEFT   JOIN sap_stg_hana.stg_sap_zmprodr_makt        d  ON d.matnr = c.matnr AND d.spras = 'E'
    LEFT   JOIN sap_stg_hana.stg_sap_zmprodr_zcustom_tab f  ON f.matnr = c.matnr
    LEFT   JOIN sap_stg_hana.stg_sap_zmprodr_mvke        g  ON g.matnr = c.matnr AND g.vkorg = '1100' AND g.vtweg = '01'
    LEFT   JOIN sap_stg_hana.stg_sap_zmprodr_tvm1t       t1 ON t1.mvgr1 = g.mvgr1 AND t1.spras = 'E'
    LEFT   JOIN sap_stg_hana.stg_sap_zmprodr_tvm2t       t2 ON t2.mvgr2 = g.mvgr2 AND t2.spras = 'E'
    LEFT   JOIN sap_stg_hana.stg_sap_zmprodr_tvm3t       t3 ON t3.mvgr3 = g.mvgr3 AND t3.spras = 'E'
    LEFT   JOIN sap_stg_hana.stg_sap_zmprodr_tvm4t       t4 ON t4.mvgr4 = g.mvgr4 AND t4.spras = 'E'
    LEFT   JOIN sap_stg_hana.stg_sap_zmprodr_tvm5t       t5 ON t5.mvgr5 = g.mvgr5 AND t5.spras = 'E'
    WHERE  (p.dauat_list IS NULL OR c.dauat = ANY (p.dauat_list))
      AND  (p.matnr_list IS NULL OR c.matnr = ANY (p.matnr_list))
),
afru_rows AS (
    SELECT i.*,
           CASE WHEN i.tmg_name = '' THEN '' ELSE coalesce(q.descr, '') END AS quality,
           CASE WHEN i.tmg_name = '' THEN ''
                ELSE coalesce(dz.descr, '') || coalesce(sh.descr, '') END   AS shade
    FROM   i_afru i
    LEFT   JOIN tmg q  ON q.tmg_name  = i.tmg_name AND q.lookup_type  = 'QLTY'   AND q.code  = i.zqlty_sort
    LEFT   JOIN tmg dz ON dz.tmg_name = i.tmg_name AND dz.lookup_type = 'DESIGN' AND dz.code = i.zdesign_no
    LEFT   JOIN tmg sh ON sh.tmg_name = i.tmg_name AND sh.lookup_type = 'SHADE'  AND sh.code = i.zshade_no
),
afru_grp AS (
    SELECT arbpl, machine, sectionname, matnr,
           min(werks) AS werks, sum(gmnga) AS gmnga, min(gmein) AS gmein, min(std_prod) AS std_prod,
           min(maktx) AS maktx, min(quality) AS quality, min(shade) AS shade,
           min(mvgr1) AS mvgr1, min(bezei1) AS bezei1, min(mvgr2) AS mvgr2, min(bezei2) AS bezei2,
           min(mvgr3) AS mvgr3, min(bezei3) AS bezei3, min(mvgr4) AS mvgr4, min(bezei4) AS bezei4,
           min(mvgr5) AS mvgr5, min(bezei5) AS bezei5
    FROM   afru_rows
    GROUP  BY arbpl, machine, sectionname, matnr
),
conf AS (                                     -- ZPPCONFIRMATION loss minutes
    SELECT z.werks, z.matnr, z.machine, z.arbpl,
           sum(COALESCE(NULLIF(regexp_replace(btrim(z.power_fail_surge), '^(.*)-$', '-\1'), '')::numeric, 0)) AS power_fail_surge,
           sum(COALESCE(NULLIF(regexp_replace(btrim(z.compressor_issue), '^(.*)-$', '-\1'), '')::numeric, 0)) AS compressor_issue,
           sum(COALESCE(NULLIF(regexp_replace(btrim(z.maint_fault),      '^(.*)-$', '-\1'), '')::numeric, 0)) AS maint_fault,
           sum(COALESCE(NULLIF(regexp_replace(btrim(z.elect_fault),      '^(.*)-$', '-\1'), '')::numeric, 0)) AS elect_fault,
           sum(COALESCE(NULLIF(regexp_replace(btrim(z.man_short),        '^(.*)-$', '-\1'), '')::numeric, 0)) AS man_short,
           sum(COALESCE(NULLIF(regexp_replace(btrim(z.spare_short),      '^(.*)-$', '-\1'), '')::numeric, 0)) AS spare_short,
           sum(COALESCE(NULLIF(regexp_replace(btrim(z.prevent_maint),    '^(.*)-$', '-\1'), '')::numeric, 0)) AS prevent_maint,
           sum(COALESCE(NULLIF(regexp_replace(btrim(z.no_plan),          '^(.*)-$', '-\1'), '')::numeric, 0)) AS no_plan,
           sum(COALESCE(NULLIF(regexp_replace(btrim(z.total_loss),       '^(.*)-$', '-\1'), '')::numeric, 0)) AS total_loss
    FROM   sap_stg_hana.stg_sap_zmprodr_zppconfirmation z
    CROSS  JOIN p
    WHERE  z.budat BETWEEN p.d_from AND p.d_to
    GROUP  BY z.werks, z.matnr, z.machine, z.arbpl
),
i_mseg AS (                                   -- AFRU -> AFWI -> MATDOC (GR 101) batches
    SELECT DISTINCT af.zzmachine, md.matnr, md.charg
    FROM   sap_stg_hana.stg_sap_zmprodr_afru   af
    JOIN   sap_stg_hana.stg_sap_zmprodr_afwi   w  ON w.rueck = af.rueck AND w.rmzhl = af.rmzhl
    JOIN   sap_stg_hana.stg_sap_zmprodr_matdoc md ON md.mblnr = w.mblnr AND md.mjahr = w.mjahr AND md.zeile = w.mblpo
    WHERE  md.bwart = '101' AND md.cancelled = ''
      AND  EXISTS (SELECT 1 FROM i_afru i WHERE i.rueck = af.rueck AND i.rmzhl = af.rmzhl)
),
batches AS (
    SELECT zzmachine, matnr,
           left(string_agg(charg, ',' ORDER BY charg COLLATE "C"), 128) AS charg
    FROM   i_mseg
    WHERE  charg <> ''
    GROUP  BY zzmachine, matnr
),
final AS (
    SELECT d.src, d.werks, d.arbpl, d.machine, d.ktext, d.sectioncode, d.sectionname,
           coalesce(g.matnr, '') AS matnr, coalesce(g.maktx, '') AS maktx,
           coalesce(g.quality, '') AS quality, coalesce(g.shade, '') AS shade,
           coalesce(g.mvgr1, '') AS mvgr1, coalesce(g.bezei1, '') AS bezei1,
           coalesce(g.mvgr2, '') AS mvgr2, coalesce(g.bezei2, '') AS bezei2,
           coalesce(g.mvgr3, '') AS mvgr3, coalesce(g.bezei3, '') AS bezei3,
           coalesce(g.mvgr4, '') AS mvgr4, coalesce(g.bezei4, '') AS bezei4,
           coalesce(g.mvgr5, '') AS mvgr5, coalesce(g.bezei5, '') AS bezei5,
           coalesce(g.std_prod, 0) * p.days AS std_prod_total,
           coalesce(g.gmein, '')            AS gmein,
           coalesce(g.gmnga, 0)             AS gmnga_raw,
           (g.machine IS NOT NULL)          AS has_afru,
           p.date_from, p.date_to
    FROM   i_data d
    CROSS  JOIN p
    LEFT   JOIN afru_grp g ON g.arbpl = d.arbpl AND g.machine = d.machine AND g.sectionname = d.sectionname
    WHERE  (p.machine_list IS NULL OR d.machine     = ANY (p.machine_list))
      AND  (p.arbpl_list   IS NULL OR d.arbpl       = ANY (p.arbpl_list))
      AND  (p.section_list IS NULL OR d.sectioncode = ANY (p.section_list))
      AND  (p.matnr_list   IS NULL OR coalesce(g.matnr, '') = ANY (p.matnr_list))
)
SELECT f.werks                         AS "Plant",
       f.date_from                     AS "From Date",
       f.date_to                       AS "To Date",
       f.arbpl                         AS "Work Center",
       f.machine                       AS "Machine No.",
       f.ktext                         AS "MCN Description",
       f.matnr                         AS "Material",
       f.maktx                         AS "Material Desc.",
       f.quality                       AS "Quality",
       f.shade                         AS "Shade",
       coalesce(b.charg, '')           AS "Batch",
       f.mvgr1                         AS "Product Hierarchy 1",
       f.bezei1                        AS "Product Hierarchy 1 desc.",
       f.mvgr2                         AS "Product Hierarchy 2",
       f.bezei2                        AS "Product Hierarchy 2 desc.",
       f.mvgr3                         AS "Product Hierarchy 3",
       f.bezei3                        AS "Product Hierarchy 3 desc",
       f.mvgr4                         AS "Product Hierarchy 4",
       f.bezei4                        AS "Product Hierarchy 4 desc",
       f.mvgr5                         AS "Product Hierarchy 5",
       f.bezei5                        AS "Product Hierarchy 5 desc",
       f.std_prod_total                AS "Std. Production",
       f.gmein                         AS "Uom",
       round(f.gmnga_raw / 10, 3)      AS "Act. Production",
       CASE WHEN f.std_prod_total <> 0
            THEN round(f.gmnga_raw / f.std_prod_total * 100, 3) ELSE 0 END AS "Achieve %",
       f.sectionname                   AS "Section",
       coalesce(c.power_fail_surge, 0) AS "POWER FAIL/SURGE",
       coalesce(c.compressor_issue, 0) AS "COMPRESSOR_ISSUE",
       coalesce(c.maint_fault, 0)      AS "Maintenance Fault",
       coalesce(c.elect_fault, 0)      AS "Electrical Fault",
       coalesce(c.man_short, 0)        AS "MANPOWER SHORTAGE",
       coalesce(c.spare_short, 0)      AS "SPARE SHORTAGE",
       coalesce(c.prevent_maint, 0)    AS "Preventive Maintenance",
       coalesce(c.no_plan, 0)          AS "No Plan",
       coalesce(c.total_loss, 0)       AS "TOTAL LOSS IN MINUTES"
FROM   final f
LEFT   JOIN conf c    ON c.werks = f.werks AND c.matnr = f.matnr
                     AND c.machine = f.machine AND c.arbpl = f.arbpl
                     AND f.has_afru
LEFT   JOIN batches b ON b.zzmachine = f.machine AND b.matnr = f.matnr
ORDER  BY f.src, f.arbpl, f.machine, f.sectioncode, f.matnr;
WITH params AS (
    SELECT DATE '2026-01-01' AS date_from,   -- S_BUDAT-LOW (NULL = no date filter)
           DATE '2026-06-30' AS date_to,     -- S_BUDAT-HIGH
           NULL::text[]      AS charg_list,  -- S_CHARG
           NULL::text[]      AS dauat_list   -- S_DAUAT (within PK01/PK02)
),
p AS (
    SELECT *, to_char(date_from, 'YYYYMMDD') AS d_from,
              to_char(coalesce(date_to, date_from), 'YYYYMMDD') AS d_to
    FROM params
),
i_matdoc AS (
    SELECT a.werks, a.aufnr,
           b.matnr,                           -- AFPO.MATNR
           coalesce(f.maktx, '') AS maktx,    -- MAKT of consumed MATDOC.MATNR
           coalesce(d.budat, '') AS budat,    -- ZPACK.BUDAT
           a.charg
    FROM   sap_stg_hana.stg_sap_zpackper_matdoc_pack a
    CROSS  JOIN p
    JOIN   sap_stg_hana.stg_sap_zpackper_afpo_pack   b ON b.aufnr = a.aufnr AND b.dauat IN ('PK01', 'PK02')
    LEFT   JOIN sap_stg_hana.stg_sap_zpackper_zpack  d ON d.aufnr = a.aufnr AND d.charg = a.charg
    LEFT   JOIN sap_stg_hana.stg_sap_zpackper_makt   f ON f.matnr = a.matnr AND f.spras = 'E'
    WHERE  a.bwart = '261' AND a.aufnr <> '' AND a.cancelled = ''
      AND  (p.d_from     IS NULL OR d.budat BETWEEN p.d_from AND p.d_to)
      AND  (p.charg_list IS NULL OR d.charg = ANY (p.charg_list))
      AND  (p.dauat_list IS NULL OR b.dauat = ANY (p.dauat_list))
),
jobcards AS (SELECT DISTINCT charg FROM i_matdoc),
weaving AS (
    SELECT w.charg, sum(COALESCE(NULLIF(regexp_replace(btrim(w.menge), '^(.*)-$', '-\1'), '')::numeric, 0)) AS weaving_mtr
    FROM   sap_stg_hana.stg_sap_zpackper_matdoc_weaving w
    JOIN   jobcards j ON j.charg = w.charg
    WHERE  w.bwart = '261' AND w.cancelled = ''
      AND  EXISTS (SELECT 1 FROM sap_stg_hana.stg_sap_zpackper_afpo_jobcard o
                   WHERE o.aufnr = w.aufnr AND o.dauat IN ('PH01', 'PH02'))
    GROUP  BY w.charg
),
finish AS (
    SELECT m.charg, sum(COALESCE(NULLIF(regexp_replace(btrim(m.menge), '^(.*)-$', '-\1'), '')::numeric, 0)) AS finish_mtr
    FROM   sap_stg_hana.stg_sap_zpackper_matdoc_finish m
    JOIN   jobcards j ON j.charg = m.charg
    WHERE  m.bwart = '101' AND m.cancelled = ''
      AND  EXISTS (SELECT 1 FROM sap_stg_hana.stg_sap_zpackper_afpo_jobcard o WHERE o.aufnr = m.aufnr)
    GROUP  BY m.charg
),
rework AS (
    SELECT r.charg, r.werks, r.matnr,
           sum(COALESCE(NULLIF(regexp_replace(btrim(r.menge), '^(.*)-$', '-\1'), '')::numeric, 0)) AS rework
    FROM   sap_stg_hana.stg_sap_zpackper_matdoc_rework r
    WHERE  r.lgort = 'FINS' AND r.umlgo = 'SFDC' AND r.shkzg = 'H'
      AND  r.bwart = '311'  AND r.cancelled = ''
    GROUP  BY r.charg, r.werks, r.matnr
),
zpack_sum AS (
    SELECT z.charg,
           sum(COALESCE(NULLIF(regexp_replace(btrim(z.pcs_1),        '^(.*)-$', '-\1'), '')::numeric, 0)) AS pcs_1,
           sum(COALESCE(NULLIF(regexp_replace(btrim(z.pcs_2),        '^(.*)-$', '-\1'), '')::numeric, 0)) AS pcs_2,
           sum(COALESCE(NULLIF(regexp_replace(btrim(z.pcs_3),        '^(.*)-$', '-\1'), '')::numeric, 0)) AS pcs_3,
           sum(COALESCE(NULLIF(regexp_replace(btrim(z.short_length), '^(.*)-$', '-\1'), '')::numeric, 0)) AS short_length,
           sum(COALESCE(NULLIF(regexp_replace(btrim(z.zsecond),      '^(.*)-$', '-\1'), '')::numeric, 0)) AS zsecond,
           sum(COALESCE(NULLIF(regexp_replace(btrim(z.rage),         '^(.*)-$', '-\1'), '')::numeric, 0)) AS rage
    FROM   sap_stg_hana.stg_sap_zpackper_zpack_all z
    GROUP  BY z.charg
),
base AS (
    SELECT i.werks, i.aufnr, i.matnr, i.maktx, i.budat, i.charg AS jobcardno,
           coalesce(w.weaving_mtr, 0)  AS wv,
           coalesce(f.finish_mtr, 0)   AS fn,
           coalesce(rw.rework, 0)      AS rw,
           coalesce(z.pcs_1, 0)        AS pcs_1,
           coalesce(z.pcs_2, 0)        AS pcs_2,
           coalesce(z.pcs_3, 0)        AS pcs_3,
           coalesce(z.short_length, 0) AS sl,
           coalesce(z.zsecond, 0)      AS sec,
           round(coalesce(z.rage, 0) * 3.6, 3) AS rags
    FROM   i_matdoc i
    LEFT   JOIN weaving   w  ON w.charg = i.charg
    LEFT   JOIN finish    f  ON f.charg = i.charg
    LEFT   JOIN rework    rw ON rw.charg = i.charg AND rw.werks = i.werks AND rw.matnr = i.matnr
    LEFT   JOIN zpack_sum z  ON z.charg = i.charg
),
dedup AS (
    SELECT DISTINCT ON (jobcardno, aufnr) *
    FROM   base
    ORDER  BY jobcardno, aufnr, budat DESC
),
calc AS (
    SELECT d.*,
           d.pcs_1 + d.pcs_2 + d.pcs_3 + d.sl + d.sec + d.rags + d.rw AS total,
           d.pcs_1 + d.pcs_2 + d.pcs_3                                AS ff
    FROM   dedup d
)
SELECT werks      AS "Plant",
       aufnr      AS "Order",
       matnr      AS "Material",
       maktx      AS "Description",
       to_date(NULLIF(NULLIF(btrim(budat), ''), '00000000'), 'YYYYMMDD') AS "Posting Date",
       jobcardno  AS "Job Card No",
       wv         AS "Weaving Mtr.",
       fn         AS "Finish Mtr.",
       CASE WHEN wv <> 0    THEN round((wv - fn) / wv * 100, 2)      ELSE 0 END AS "Srinkage",
       pcs_1      AS "1 PCS",
       pcs_2      AS "2 PCS",
       pcs_3      AS "3 PCS",
       sl         AS "Short Length",
       sec        AS "Second",
       rags       AS "Rags",
       total      AS "Total",
       ff         AS "Fresh Fabric",
       CASE WHEN ff <> 0    THEN round(fn / ff, 3)                   ELSE 0 END AS "Fresh",
       CASE WHEN fn <> 0    THEN round((sl + sec) / fn, 3)           ELSE 0 END AS "Short Length & Second",
       CASE WHEN fn <> 0    THEN round(rags / fn, 3)                 ELSE 0 END AS "Rage",
       CASE WHEN total <> 0 THEN round(fn / total - 1, 3)            ELSE 0 END AS "Varient",
       CASE WHEN wv <> 0    THEN round(total / wv * 100, 3)          ELSE 0 END AS "Weaving to Packing",
       CASE WHEN wv <> 0    THEN round(ff / wv * 100, 2)             ELSE 0 END AS "Fresh%ge wrt Wvng Mtr",
       CASE WHEN wv <> 0    THEN round((sl + sec) / wv * 100, 2)     ELSE 0 END AS "SL & Second %ge wrt Wvng Mtr",
       CASE WHEN wv <> 0    THEN round(rags / wv * 100, 2)           ELSE 0 END AS "Rags %ge wrt Wvng Mtr",
       CASE WHEN wv <> 0    THEN round(rw / wv * 100, 2)             ELSE 0 END AS "Rework %ge wrt Wvng Mtr",
       CASE WHEN wv <> 0    THEN round((wv - total) / wv * 100, 2)   ELSE 0 END AS "Variance %ge wrt Wvng Mtr",
       CASE WHEN fn <> 0    THEN round(ff / fn * 100, 2)             ELSE 0 END AS "Fresh%ge wrt Finish Mtr",
       CASE WHEN fn <> 0    THEN round((sl + sec) / fn * 100, 2)     ELSE 0 END AS "SL & Second %ge wrt Finish Mtr",
       CASE WHEN fn <> 0    THEN round(rags / fn * 100, 2)           ELSE 0 END AS "Rags %ge wrt Finish Mtr",
       CASE WHEN fn <> 0    THEN round(rw / fn * 100, 2)             ELSE 0 END AS "Rework %ge wrt Finish Mtr",
       CASE WHEN fn <> 0    THEN round((fn - total) / fn * 100, 2)   ELSE 0 END AS "Variance %ge wrt Finish Mtr",
       rw         AS "Rework"
FROM   calc
ORDER  BY budat DESC, jobcardno, aufnr;
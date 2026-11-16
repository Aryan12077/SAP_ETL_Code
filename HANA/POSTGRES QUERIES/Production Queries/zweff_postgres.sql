WITH params AS (
    SELECT DATE '2026-01-01' AS date_from,
           DATE '2026-06-30' AS date_to,
           NULL::text[]      AS section_list,          -- NULL = all sections
           false             AS include_unsaved_looms  -- true = also list ZLOOM looms with no entry that day
),
p AS (
    SELECT *, to_char(date_from, 'YYYYMMDD') AS d_from, to_char(date_to, 'YYYYMMDD') AS d_to
    FROM params
),
sect_txt AS (
    SELECT DISTINCT ON (domvalue_l) domvalue_l, ddtext
    FROM   sap_stg_hana.stg_sap_zweff_dd07t
    WHERE  domname = 'ZSACTIONNAME' AND ddlanguage = 'E'
    ORDER  BY domvalue_l, valpos
),
saved AS (
    SELECT z.*
    FROM   sap_stg_hana.stg_sap_zweff_zweff z
    CROSS  JOIN p
    WHERE  z.datum BETWEEN p.d_from AND p.d_to
      AND  (p.section_list IS NULL OR z.sectionname = ANY (p.section_list))
),
unsaved AS (
    SELECT l.werks, to_char(d.dt, 'YYYYMMDD') AS datum, l.sectionname, l.loomno, l.rpm
    FROM   p
    CROSS  JOIN generate_series(p.date_from, p.date_to, interval '1 day') AS d(dt)
    JOIN   sap_stg_hana.stg_sap_zweff_zloom l ON (p.section_list IS NULL OR l.sectionname = ANY (p.section_list))
    WHERE  p.include_unsaved_looms
      AND  NOT EXISTS (SELECT 1 FROM sap_stg_hana.stg_sap_zweff_zweff z
                       WHERE z.werks = l.werks AND z.sectionname = l.sectionname
                         AND z.datum = to_char(d.dt, 'YYYYMMDD'))
),
rows_ AS (
    SELECT werks, datum, sectionname, loomno, matnr,
           COALESCE(NULLIF(regexp_replace(btrim(rpm),               '^(.*)-$', '-\1'), '')::numeric, 0) AS rpm,
           COALESCE(NULLIF(regexp_replace(btrim(width),             '^(.*)-$', '-\1'), '')::numeric, 0) AS width,
           COALESCE(NULLIF(regexp_replace(btrim(ppi),               '^(.*)-$', '-\1'), '')::numeric, 0) AS ppi,
           COALESCE(NULLIF(regexp_replace(btrim(mcn),               '^(.*)-$', '-\1'), '')::numeric, 0) AS mcn,
           COALESCE(NULLIF(regexp_replace(btrim(shift_a_mtr),       '^(.*)-$', '-\1'), '')::numeric, 0) AS shift_a_mtr,
           COALESCE(NULLIF(regexp_replace(btrim(picks_a_mtr),       '^(.*)-$', '-\1'), '')::numeric, 0) AS picks_a_mtr,
           COALESCE(NULLIF(regexp_replace(btrim(effi_a_mtr),        '^(.*)-$', '-\1'), '')::numeric, 0) AS effi_a_mtr,
           COALESCE(NULLIF(regexp_replace(btrim(shift_b_mtr),       '^(.*)-$', '-\1'), '')::numeric, 0) AS shift_b_mtr,
           COALESCE(NULLIF(regexp_replace(btrim(picks_b_mtr),       '^(.*)-$', '-\1'), '')::numeric, 0) AS picks_b_mtr,
           COALESCE(NULLIF(regexp_replace(btrim(effi_b_mtr),        '^(.*)-$', '-\1'), '')::numeric, 0) AS effi_b_mtr,
           COALESCE(NULLIF(regexp_replace(btrim(shift_c_mtr),       '^(.*)-$', '-\1'), '')::numeric, 0) AS shift_c_mtr,
           COALESCE(NULLIF(regexp_replace(btrim(picks_c_mtr),       '^(.*)-$', '-\1'), '')::numeric, 0) AS picks_c_mtr,
           COALESCE(NULLIF(regexp_replace(btrim(effi_c_mtr),        '^(.*)-$', '-\1'), '')::numeric, 0) AS effi_c_mtr,
           COALESCE(NULLIF(regexp_replace(btrim(total_prod),        '^(.*)-$', '-\1'), '')::numeric, 0) AS total_prod,
           COALESCE(NULLIF(regexp_replace(btrim(total_pick),        '^(.*)-$', '-\1'), '')::numeric, 0) AS total_pick,
           COALESCE(NULLIF(regexp_replace(btrim(total_effi),        '^(.*)-$', '-\1'), '')::numeric, 0) AS total_effi,
           COALESCE(NULLIF(regexp_replace(btrim(run_min),           '^(.*)-$', '-\1'), '')::numeric, 0) AS run_min,
           COALESCE(NULLIF(regexp_replace(btrim(ktg_gtg),           '^(.*)-$', '-\1'), '')::numeric, 0) AS ktg_gtg,
           COALESCE(NULLIF(regexp_replace(btrim(power_fail),        '^(.*)-$', '-\1'), '')::numeric, 0) AS power_fail,
           COALESCE(NULLIF(regexp_replace(btrim(maint_issue),       '^(.*)-$', '-\1'), '')::numeric, 0) AS maint_issue,
           COALESCE(NULLIF(regexp_replace(btrim(no_plan),           '^(.*)-$', '-\1'), '')::numeric, 0) AS no_plan,
           COALESCE(NULLIF(regexp_replace(btrim(raw_mat_issue),     '^(.*)-$', '-\1'), '')::numeric, 0) AS raw_mat_issue,
           COALESCE(NULLIF(regexp_replace(btrim(spare_issue),       '^(.*)-$', '-\1'), '')::numeric, 0) AS spare_issue,
           COALESCE(NULLIF(regexp_replace(btrim(others),            '^(.*)-$', '-\1'), '')::numeric, 0) AS others,
           COALESCE(NULLIF(regexp_replace(btrim(break_cmpx),        '^(.*)-$', '-\1'), '')::numeric, 0) AS break_cmpx,
           COALESCE(NULLIF(regexp_replace(btrim(shift_a_weft_cmpx), '^(.*)-$', '-\1'), '')::numeric, 0) AS shift_a_weft_cmpx,
           COALESCE(NULLIF(regexp_replace(btrim(shift_a_warp_cmpx), '^(.*)-$', '-\1'), '')::numeric, 0) AS shift_a_warp_cmpx,
           COALESCE(NULLIF(regexp_replace(btrim(shift_b_weft_cmpx), '^(.*)-$', '-\1'), '')::numeric, 0) AS shift_b_weft_cmpx,
           COALESCE(NULLIF(regexp_replace(btrim(shift_b_warp_cmpx), '^(.*)-$', '-\1'), '')::numeric, 0) AS shift_b_warp_cmpx,
           COALESCE(NULLIF(regexp_replace(btrim(shift_c_weft_cmpx), '^(.*)-$', '-\1'), '')::numeric, 0) AS shift_c_weft_cmpx,
           COALESCE(NULLIF(regexp_replace(btrim(shift_c_warp_cmpx), '^(.*)-$', '-\1'), '')::numeric, 0) AS shift_c_warp_cmpx,
           COALESCE(NULLIF(regexp_replace(btrim(oper_eff),          '^(.*)-$', '-\1'), '')::numeric, 0) AS oper_eff,
           operatorcode1, operatorcode2, operatorcode3, operatorcode4, operatorcode5, operatorcode6
    FROM   saved
    UNION ALL
    SELECT werks, datum, sectionname, loomno, '',
           COALESCE(NULLIF(regexp_replace(btrim(rpm), '^(.*)-$', '-\1'), '')::numeric, 0),
           0, 0, 0,  0, 0, 0,  0, 0, 0,  0, 0, 0,  0, 0, 0,
           0, 0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0,  0,
           '', '', '', '', '', ''
    FROM   unsaved
)
SELECT r.werks                 AS "Plant",
       to_date(NULLIF(NULLIF(btrim(r.datum), ''), '00000000'), 'YYYYMMDD') AS "Date",
       r.sectionname           AS "Section",
       coalesce(t.ddtext, '')  AS "Section Name",
       r.loomno                AS "Loom No.",
       r.matnr                 AS "Material",
       coalesce(m.maktx, '')   AS "Material Desc.",
       r.rpm                   AS "RPM",
       r.width                 AS "Width",
       r.ppi                   AS "PPI",
       r.mcn                   AS "MCN",
       r.shift_a_mtr           AS "Shift A Mtr",
       r.picks_a_mtr           AS "Picks A",
       r.effi_a_mtr            AS "Effi. A %",
       r.shift_b_mtr           AS "Shift B Mtr",
       r.picks_b_mtr           AS "Picks B",
       r.effi_b_mtr            AS "Effi. B %",
       r.shift_c_mtr           AS "Shift C Mtr",
       r.picks_c_mtr           AS "Picks C",
       r.effi_c_mtr            AS "Effi. C %",
       r.total_prod            AS "Total Prod.",
       r.total_pick            AS "Total Picks",
       r.total_effi            AS "Total Effi. %",
       r.run_min               AS "Run Min",
       r.ktg_gtg               AS "KTG/GTG",
       r.power_fail            AS "Power Fail",
       r.maint_issue           AS "Maint. Issue",
       r.no_plan               AS "No Plan",
       r.raw_mat_issue         AS "Raw Mat. Issue",
       r.spare_issue           AS "Spare Issue",
       r.others                AS "Others",
       r.break_cmpx            AS "Break CMPX",
       r.shift_a_weft_cmpx     AS "Shift A Weft CMPX",
       r.shift_a_warp_cmpx     AS "Shift A Warp CMPX",
       r.shift_b_weft_cmpx     AS "Shift B Weft CMPX",
       r.shift_b_warp_cmpx     AS "Shift B Warp CMPX",
       r.shift_c_weft_cmpx     AS "Shift C Weft CMPX",
       r.shift_c_warp_cmpx     AS "Shift C Warp CMPX",
       r.oper_eff              AS "Oper. Eff. %",
       r.operatorcode1         AS "Operator 1",
       r.operatorcode2         AS "Operator 2",
       r.operatorcode3         AS "Operator 3",
       r.operatorcode4         AS "Operator 4",
       r.operatorcode5         AS "Operator 5",
       r.operatorcode6         AS "Operator 6"
FROM   rows_ r
LEFT   JOIN sect_txt t ON t.domvalue_l = r.sectionname
LEFT   JOIN sap_stg_hana.stg_sap_zweff_makt m ON m.matnr = r.matnr AND m.spras = 'E'
ORDER  BY r.datum, r.werks, r.sectionname, r.loomno;
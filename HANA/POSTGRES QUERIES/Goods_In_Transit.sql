WITH params AS (
    -- Edit these two lines. Use CAST(NULL AS text[]) for "no filter".
    -- Example: ARRAY['4500000001','4500000002']
    SELECT CAST(NULL AS text[]) AS ebeln_list,
           CAST(NULL AS text[]) AS vbeln_list
),
bseg_first AS (
    SELECT bukrs, belnr, gjahr, dmbtr, rfccur,
           ROW_NUMBER() OVER (
               PARTITION BY bukrs, belnr, gjahr
               ORDER BY buzei
           ) AS rn
    FROM   sap_stg_hana.stg_sap_git_bseg
    WHERE  shkzg = 'S'
),
base AS (
    SELECT DISTINCT
           a.bukrs, a.ebeln, a.vbeln, a.lifnr, b.name1,
           a.wkurs, a.waers, a.inv_val, a.bldat,
           a.belnr, c.blart, a.gjahr, a.mblnr, a.mjahr,
           CASE WHEN TRIM(COALESCE(a.rev_belnr, '')) = ''
                THEN c.stblg
                ELSE a.rev_belnr
           END AS rev_belnr,
           CASE WHEN TRIM(COALESCE(a.rev_belnr, '')) = ''
                 AND TRIM(COALESCE(c.stblg, '')) <> ''
                THEN 'Goods Arrived'
                ELSE a.git_status
           END AS git_status
    FROM   sap_stg_hana.stg_sap_git_zgit a
    INNER JOIN sap_stg_hana.stg_sap_git_lfa1 b
            ON b.lifnr = a.lifnr
    INNER JOIN sap_stg_hana.stg_sap_git_bkpf c
            ON c.bukrs = a.bukrs
           AND c.belnr = a.belnr
           AND c.gjahr = a.gjahr
    CROSS JOIN params p
    WHERE  (p.ebeln_list IS NULL OR a.ebeln = ANY(p.ebeln_list))
      AND  (p.vbeln_list IS NULL OR a.vbeln = ANY(p.vbeln_list))
),
typed AS (
    SELECT t.*,
           -- SAP numbers: plain, leading minus, or trailing minus ("100.00-")
           CASE WHEN TRIM(t.wkurs) ~ '^-?[0-9]+([.][0-9]+){0,1}$'
                     THEN CAST(TRIM(t.wkurs) AS numeric)
                WHEN TRIM(t.wkurs) ~ '^[0-9]+([.][0-9]+){0,1}-$'
                     THEN -CAST(REPLACE(TRIM(t.wkurs), '-', '') AS numeric)
           END AS wkurs_n,
           CASE WHEN TRIM(t.inv_val) ~ '^-?[0-9]+([.][0-9]+){0,1}$'
                     THEN CAST(TRIM(t.inv_val) AS numeric)
                WHEN TRIM(t.inv_val) ~ '^[0-9]+([.][0-9]+){0,1}-$'
                     THEN -CAST(REPLACE(TRIM(t.inv_val), '-', '') AS numeric)
           END AS inv_val_n,
           CASE WHEN TRIM(s.dmbtr) ~ '^-?[0-9]+([.][0-9]+){0,1}$'
                     THEN CAST(TRIM(s.dmbtr) AS numeric)
                WHEN TRIM(s.dmbtr) ~ '^[0-9]+([.][0-9]+){0,1}-$'
                     THEN -CAST(REPLACE(TRIM(s.dmbtr), '-', '') AS numeric)
           END AS dmbtr_n,
           CASE WHEN t.bldat ~ '^[0-9]{8}$' AND t.bldat <> '00000000'
                THEN to_date(t.bldat, 'YYYYMMDD')
           END AS bldat_d,
           s.belnr  AS s_belnr,
           s.rfccur AS s_rfccur,
           (s.belnr IS NOT NULL AND TRIM(COALESCE(t.rev_belnr, '')) <> '') AS is_rev
    FROM   base t
    LEFT JOIN bseg_first s
           ON s.bukrs = t.bukrs
          AND s.belnr = t.belnr
          AND s.gjahr = t.gjahr
          AND s.rn = 1
)
SELECT
    bukrs                                        AS "Company Code",
    ebeln                                        AS "PO Number",
    vbeln                                        AS "Inbound Delivery",
    lifnr                                        AS "Vendor Code",
    name1                                        AS "Vendor Code Name",
    wkurs_n                                      AS "Exchange Rate",
    waers                                        AS "PO Currency",
    inv_val_n                                    AS "Inbound Value",
    dmbtr_n                                      AS "Amount In Local Currency",
    s_rfccur                                     AS "Currency",
    bldat_d                                      AS "Inbound Date",
    belnr                                        AS "Inbound Accounting Document",
    blart                                        AS "Document Type",
    gjahr                                        AS "Inbound Accounting Year",
    mblnr                                        AS "GRN Document",
    mjahr                                        AS "GRN Year",
    rev_belnr                                    AS "GRN Reversal Accounting Entry",
    git_status                                   AS "GIT Status",
    CASE WHEN is_rev THEN inv_val_n * -1 END     AS "Reverse Amount Document currency",
    CASE WHEN is_rev THEN dmbtr_n  * -1 END      AS "Reverse Amount In Local Currency",
    CASE WHEN is_rev THEN s_rfccur END           AS "Reverse Currency"
FROM   typed
ORDER BY ebeln, vbeln

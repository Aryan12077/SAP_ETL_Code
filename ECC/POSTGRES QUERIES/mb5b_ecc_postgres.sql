/* ============================================================
    MB5B — Stock Movement Overview | PostgreSQL
   ============================================================
   Uses parameterised dates via a CTE instead of DECLARE. */

WITH CTE_PARAMS AS (
    SELECT '20251231' AS date_from, '20251231' AS date_to
)

SELECT

    mseg.werks                                      AS "ValA",
    t001.butxt                                      AS "Company Name",

    /* Remove leading zeros from material number */
    LTRIM(mseg.matnr, '0')                          AS "Material",

    makt.maktx                                      AS "Material Description",

    TO_CHAR(TO_DATE(p.date_from, 'YYYYMMDD'), 'DD.MM.YYYY') AS "From Date",
    TO_CHAR(TO_DATE(p.date_to,   'YYYYMMDD'), 'DD.MM.YYYY') AS "To Date",

    /* ── Opening Qty ─────────────────────────────────────────── */
    SUM(CASE
        WHEN mkpf.budat < p.date_from
        THEN CASE
                WHEN mseg.shkzg = 'H' THEN -CAST(NULLIF(mseg.menge, '') AS NUMERIC(18,3))
                ELSE CAST(NULLIF(mseg.menge, '') AS NUMERIC(18,3))
             END
        ELSE 0
    END)                                            AS "Opening Stock",

    /* ── Receipt Qty ─────────────────────────────────────────── */
    SUM(CASE
        WHEN mkpf.budat BETWEEN p.date_from AND p.date_to
             AND mseg.shkzg = 'S'
        THEN CAST(NULLIF(mseg.menge, '') AS NUMERIC(18,3))
        ELSE 0
    END)                                            AS "Total Receipt Qties",

    /* ── Issue Qty ───────────────────────────────────────────── */
    SUM(CASE
        WHEN mkpf.budat BETWEEN p.date_from AND p.date_to
             AND mseg.shkzg = 'H'
        THEN -CAST(NULLIF(mseg.menge, '') AS NUMERIC(18,3))
        ELSE 0
    END)                                            AS "Total Issue Quantities",

    /* ── Closing Qty ─────────────────────────────────────────── */
    SUM(CASE
        WHEN mkpf.budat <= p.date_to
        THEN CASE
                WHEN mseg.shkzg = 'H' THEN -CAST(NULLIF(mseg.menge, '') AS NUMERIC(18,3))
                ELSE CAST(NULLIF(mseg.menge, '') AS NUMERIC(18,3))
             END
        ELSE 0
    END)                                            AS "Closing Stock",

    mseg.meins                                      AS "BUn",

    /* ── Opening Value ───────────────────────────────────────── */
    SUM(CASE
        WHEN mkpf.budat < p.date_from
        THEN CASE
                WHEN mseg.shkzg = 'H' THEN -CAST(NULLIF(mseg.dmbtr, '') AS NUMERIC(18,2))
                ELSE CAST(NULLIF(mseg.dmbtr, '') AS NUMERIC(18,2))
             END
        ELSE 0
    END)                                            AS "Opening Value",

    /* ── Receipt Value ───────────────────────────────────────── */
    SUM(CASE
        WHEN mkpf.budat BETWEEN p.date_from AND p.date_to
             AND mseg.shkzg = 'S'
        THEN CAST(NULLIF(mseg.dmbtr, '') AS NUMERIC(18,2))
        ELSE 0
    END)                                            AS "Total Receipt Values",

    /* ── Issue Value ─────────────────────────────────────────── */
    SUM(CASE
        WHEN mkpf.budat BETWEEN p.date_from AND p.date_to
             AND mseg.shkzg = 'H'
        THEN -CAST(NULLIF(mseg.dmbtr, '') AS NUMERIC(18,2))
        ELSE 0
    END)                                            AS "Total Issue Values",

    /* ── Closing Value ───────────────────────────────────────── */
    SUM(CASE
        WHEN mkpf.budat <= p.date_to
        THEN CASE
                WHEN mseg.shkzg = 'H' THEN -CAST(NULLIF(mseg.dmbtr, '') AS NUMERIC(18,2))
                ELSE CAST(NULLIF(mseg.dmbtr, '') AS NUMERIC(18,2))
             END
        ELSE 0
    END)                                            AS "Closing Value",

    mseg.waers                                      AS "Crcy"

FROM "sap_stg_ecc"."stg_sap_mb5b_mseg" mseg

CROSS JOIN CTE_PARAMS p

INNER JOIN "sap_stg_ecc"."stg_sap_mb5b_mkpf" mkpf
    ON mkpf.mblnr = mseg.mblnr
    AND mkpf.mjahr = mseg.mjahr

LEFT JOIN "sap_stg_ecc"."stg_sap_mb5b_makt" makt
    ON makt.matnr = mseg.matnr
    AND makt.spras = 'E'

LEFT JOIN "sap_stg_ecc"."stg_sap_mb5b_t001" t001
    ON t001.bukrs = mseg.bukrs

WHERE mkpf.budat <= p.date_to

GROUP BY
    mseg.werks,
    t001.butxt,
    mseg.matnr,
    makt.maktx,
    mseg.meins,
    mseg.waers,
    p.date_from,
    p.date_to

ORDER BY
    mseg.werks,
    mseg.matnr;

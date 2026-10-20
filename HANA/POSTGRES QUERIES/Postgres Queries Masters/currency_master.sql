WITH cte_used_currency AS (
    SELECT DISTINCT TRIM(waers) AS currency_code
    FROM "sap_stg_hana"."stg_sap_currency_master_t001"
    WHERE TRIM(waers) <> ''
)

SELECT
    u.currency_code                                         AS currency_code,
    TRIM(t.ltext)                                           AS currency,

    COALESCE(CAST(NULLIF(TRIM(x.currdec), '') AS INTEGER), 2)
                                                            AS dividend,

    SUBSTRING(TRIM(t.ltext) FROM '[^ ]+$')                  AS dividend_abbreviation,

    'UNICHAR(' || m.unicode_codepoint || ')'                AS currency_unichar

FROM cte_used_currency u

INNER JOIN "sap_stg_hana"."stg_sap_currency_master_tcurc" c
    ON TRIM(c.waers) = u.currency_code

LEFT JOIN "sap_stg_hana"."stg_sap_currency_master_tcurt" t
    ON TRIM(t.waers) = u.currency_code
   AND t.spras = 'E'

LEFT JOIN "sap_stg_hana"."stg_sap_currency_master_tcurx" x
    ON TRIM(x.currkey) = u.currency_code

LEFT JOIN sap_stg_hana.sap_ref_currency_symbol m
    ON m.currency_code = u.currency_code

ORDER BY
    u.currency_code;
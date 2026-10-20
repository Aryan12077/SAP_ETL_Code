SELECT
    TRIM(c.rcomp)   AS group_company_code,
    TRIM(g.name1)   AS group_company_name,
    TRIM(c.bukrs)   AS company_code,
    TRIM(c.butxt)   AS company_name,
    TRIM(c.waers)   AS base_currency_code

FROM "sap_stg_ecc"."stg_sap_company_master_t001" c

INNER JOIN "sap_stg_ecc"."stg_sap_company_master_t880" g
    ON g.rcomp = c.rcomp

ORDER BY
    c.bukrs;
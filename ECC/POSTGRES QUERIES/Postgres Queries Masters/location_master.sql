WITH cte_address AS (
    -- ADRC can hold several rows per address number (versions / nations); keep one
    SELECT DISTINCT ON (addrnumber)
        addrnumber,
        name1,
        street,
        city1,
        post_code1,
        country,
        region
    FROM "sap_stg_ecc"."stg_sap_location_master_adrc"
    ORDER BY addrnumber, name1, street, city1
)

SELECT
    COALESCE(c.rcomp, '')                       AS "group_company_code",
    COALESCE(k.bukrs, '')                       AS "company_code",

    w.werks                                     AS "branch_id",
    w.name1                                     AS "branch",

    COALESCE(l.lgort, '')                       AS "location_code",
    COALESCE(l.lgobe, '')                       AS "location",

    CASE
        WHEN w.name1 ILIKE '%bond%' THEN 'Bonded Warehouse'
        ELSE 'Branch'
    END                                         AS "location_type",

    'N/A'                                       AS "location_category",

    COALESCE(a.name1, '')                       AS "address_name",
    COALESCE(a.street, '')                      AS "street",
    COALESCE(a.city1, '')                       AS "city",
    COALESCE(a.post_code1, '')                  AS "postal_code",
    COALESCE(a.country, '')                     AS "country",
    COALESCE(a.region, '')                      AS "region"

FROM "sap_stg_ecc"."stg_sap_location_master_t001w" w

LEFT JOIN "sap_stg_ecc"."stg_sap_location_master_t001k" k
    ON k.bwkey = w.bwkey

LEFT JOIN "sap_stg_ecc"."stg_sap_location_master_t001" c
    ON c.bukrs = k.bukrs

LEFT JOIN "sap_stg_ecc"."stg_sap_location_master_t001l" l
    ON l.werks = w.werks

LEFT JOIN cte_address a
    ON a.addrnumber = w.adrnr

ORDER BY
    k.bukrs,
    w.werks,
    l.lgort;
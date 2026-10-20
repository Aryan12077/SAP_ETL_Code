/* ============================================================
    Asset Register | PostgreSQL
   ============================================================ */

WITH CTE_ORG AS (
    SELECT
        bukrs,
        anln1,
        anln2,
        gsber                   AS business_area,
        kostl                   AS cost_center,
        werks                   AS plant,
        stort                   AS asset_location,
        prctr                   AS profit_center,
        segment                 AS segment,
        adatu                   AS validity_from,
        bdatu                   AS validity_to
    FROM "sap_stg_ecc"."stg_sap_asset_anlz"
),

CTE_VALUES AS (
    SELECT
        bukrs,
        anln1,
        anln2,
        gjahr                                               AS fiscal_year,
        afabe                                               AS dep_area,
        CAST(NULLIF(kansw, '') AS NUMERIC(18,2))            AS acquis_val,
        CAST(NULLIF(knafa, '') AS NUMERIC(18,2))            AS accum_dep,
        CAST(NULLIF(kansw, '') AS NUMERIC(18,2))
            + CAST(NULLIF(knafa, '') AS NUMERIC(18,2))      AS book_val,
        CAST(NULLIF(nafag, '') AS NUMERIC(18,2))            AS dep_posted_ytd,
        CAST(NULLIF(answl, '') AS NUMERIC(18,2))            AS additions_ytd,
        CAST(NULLIF(abgan, '') AS NUMERIC(18,2))            AS retirements_ytd,
        CAST(NULLIF(nafap, '') AS NUMERIC(18,2))            AS planned_dep_full_year,
        afblpe                                              AS last_dep_period
    FROM "sap_stg_ecc"."stg_sap_asset_anlc"
),

CTE_ACCT AS (
    SELECT
        ktopl,
        ktogr,
        afabe,
        ktansw                  AS bal_sh_acct_apc,
        ktmind                  AS retirement_loss_acct,
        ktmehr                  AS retirement_gain_acct
    FROM "sap_stg_ecc"."stg_sap_asset_t095"
),

CTE_STATUS AS (
    SELECT
        bukrs,
        anln1,
        anln2,
        CASE
            WHEN xloev = 'X'                     THEN 'Deletion Flag'
            WHEN NULLIF(deakt,'') IS NOT NULL     THEN 'Retired'
            ELSE 'Active'
        END                     AS asset_status,
        NULLIF(deakt, '')       AS deactivation_date
    FROM "sap_stg_ecc"."stg_sap_asset_anla"
)

/* ──────────────────────────────────────────────────────────────
    MAIN SELECT
   ────────────────────────────────────────────────────────────── */
SELECT

    an.anln1                                        AS "Asset",
    an.anln2                                        AS "Subnumber",
    an.bukrs                                        AS "Company Code",

    vc.dep_area                                     AS "Balance sheet item",
    an.anlkl                                        AS "Asset Class",
    an.aktiv                                        AS "Capitalized on",

    an.txt50                                        AS "Asset description",
    an.txa50                                        AS "Additional description",

    og.business_area                                AS "Business Area",
    og.cost_center                                  AS "Cost Center",
    og.plant                                        AS "Plant",
    og.asset_location                               AS "Location",
    og.profit_center                                AS "Profit Center",
    og.segment                                      AS "Segment",

    ac.bal_sh_acct_apc                              AS "Bal.sh.acct APC",

    vc.acquis_val                                   AS "Acquis.val.",
    vc.accum_dep                                    AS "Accum.dep.",
    vc.book_val                                     AS "Book val.",

    'UGX'                                           AS "Currency",

    vc.fiscal_year                                  AS "Fiscal Year",
    vc.additions_ytd                                AS "Additions YTD",
    vc.retirements_ytd                              AS "Retirements YTD",
    vc.dep_posted_ytd                               AS "Dep. Posted YTD",
    vc.planned_dep_full_year                        AS "Planned Dep. Full Year",
    vc.last_dep_period                              AS "Last Dep. Period",

    st.asset_status                                 AS "Status",
    st.deactivation_date                            AS "Deactivation Date",

    an.invnr                                        AS "Inventory Number",
    an.sernr                                        AS "Serial Number",
    an.lifnr                                        AS "Vendor",
    an.liefe                                        AS "Supplier Name",
    an.herst                                        AS "Manufacturer",
    an.eaufn                                        AS "Investment Order",
    an.ernam                                        AS "Created By",
    an.erdat                                        AS "Created On",
    an.aenam                                        AS "Changed By",
    an.aedat                                        AS "Changed On",
    an.ktogr                                        AS "Account Determination"

FROM      "sap_stg_ecc"."stg_sap_asset_anla"         an

LEFT JOIN CTE_ORG                       og
    ON  og.bukrs  = an.bukrs
    AND og.anln1  = an.anln1
    AND og.anln2  = an.anln2

LEFT JOIN CTE_VALUES                    vc
    ON  vc.bukrs  = an.bukrs
    AND vc.anln1  = an.anln1
    AND vc.anln2  = an.anln2

LEFT JOIN CTE_ACCT                      ac
    ON  ac.ktogr  = an.ktogr
    AND ac.afabe  = vc.dep_area

LEFT JOIN CTE_STATUS                    st
    ON  st.bukrs  = an.bukrs
    AND st.anln1  = an.anln1
    AND st.anln2  = an.anln2

ORDER BY
    an.anln1   ASC,
    an.anln2   ASC;

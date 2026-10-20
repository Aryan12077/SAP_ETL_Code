INSERT INTO sap_stg_ecc."SAP_MATERIAL_MASTER_FINAL" (
    company_code, branch_id, product_code,
    "SIZE", product,
    unit_code, unit, weight_unit, weight_pcs,
    creation_date, brand_code,
    reorder_level, min_level, max_level, safety_stock, minimum_safety_stock,
    lead_time_in_days, abc,
    price_control, standard_price, moving_avg_price, price_unit, price,
    unit_price_raw, currency, currdec_applied, unit_price_adjusted,
    material_group_code, level_1_category, level_2_division,
    level_3_subcategory, material_group_desc, sap_matkl_full_desc,
    "Material Type", "Material Type Description"
)
WITH cleaned_t023t AS (
    SELECT 
        TRIM(t.mandt) AS mandt,
        TRIM(t.spras) AS spras,
        TRIM(t.matkl) AS matkl,
        TRIM(t.wgbez) AS short_desc,
        TRIM(t.wgbez60) AS long_desc,
        d.raw_full_desc,
        -- Strip out the leading prefix (e.g., 'FG:', 'RM:SP:') for a clean item name
        TRIM(REGEXP_REPLACE(d.raw_full_desc, '^(TG|FG|WM|SS|RM|PM|SC|SF|SP|OS|FA|ES|CS|WIP)(:[A-Z]{2})?:\s*', '')) AS clean_item_desc
    FROM sap_stg_ecc."stg_sap_MM_t023t" t
    CROSS JOIN LATERAL (
        SELECT COALESCE(
            NULLIF(TRIM(t.wgbez60), ''), 
            NULLIF(TRIM(t.wgbez), ''), 
            'Unclassified'
        ) AS raw_full_desc
    ) d
    WHERE TRIM(t.spras) = 'E'
      AND TRIM(t.matkl) <> '444444444'
),

material_group_hierarchy AS (
    SELECT 
        mandt,
        matkl,
        short_desc,
        long_desc,
        raw_full_desc,
        clean_item_desc,

        -- LEVEL 1: Material Category
        CASE 
            WHEN matkl IN ('392060', '392070', '392080') THEN 'Raw Materials (RM)'
            WHEN matkl LIKE '11%' THEN 'Trading Goods (TG)'
            WHEN matkl LIKE '20%' THEN 'Finished Goods (FG)'
            WHEN matkl LIKE '28%' THEN 'Waste Materials (WM)'
            WHEN matkl LIKE '29%' THEN 'Subcontracting & Sales Services (SS)'
            WHEN matkl LIKE '30%' THEN 'Raw Materials (RM)'
            WHEN matkl LIKE '37%' THEN 'Packing Materials (PM)'
            WHEN matkl LIKE '39%' THEN 'Subcontracting / Semi-Finished (SC)'
            WHEN matkl LIKE '40%' THEN 'Semi-Finished Goods / WIP (SF)'
            WHEN matkl LIKE '50%' THEN 'Spare Parts (SP)'
            WHEN SUBSTRING(matkl, 1, 3) BETWEEN '600' AND '607' THEN 'Operating Supplies & Consumables (OS)'
            WHEN SUBSTRING(matkl, 1, 3) BETWEEN '608' AND '609' THEN 'Fixed Assets (FA)'
            WHEN SUBSTRING(matkl, 1, 3) BETWEEN '700' AND '707' THEN 'Expenses & General Services (ES)'
            WHEN SUBSTRING(matkl, 1, 3) BETWEEN '708' AND '709' THEN 'Capital Services / CWIP (CS)'
            WHEN matkl LIKE 'ESN-%'  THEN 'Expenses & General Services (ES)'
            WHEN matkl LIKE 'FG%'    THEN 'Finished Goods (FG)'
            WHEN matkl LIKE 'SF%'    THEN 'Semi-Finished Goods / WIP (SF)'
            WHEN matkl LIKE 'RM%'    THEN 'Raw Materials (RM)'
            WHEN matkl LIKE 'PAC%'   THEN 'Packing Materials (PM)'
            WHEN matkl LIKE 'OS%'    THEN 'Operating Supplies & Consumables (OS)'
            WHEN matkl LIKE 'SP%'    THEN 'Spare Parts (SP)'
            WHEN matkl LIKE 'SR%' OR matkl LIKE 'JBF%' THEN 'Subcontracting & Sales Services (SS)'
            WHEN matkl LIKE 'TRMG-%' THEN 'Trading Goods (TG)'
            ELSE 'Other / Unclassified'
        END AS level_1_category,

        -- LEVEL 2: Business Division / Vertical
        CASE 
            WHEN matkl IN ('202700', '302400', '402400') THEN 'Power & Energy'
            WHEN matkl LIKE '11%' THEN 'General Trading'
            WHEN SUBSTRING(matkl, 1, 2) IN ('20', '28', '29', '30', '39', '40') AND SUBSTRING(matkl, 3, 1) = '1' THEN 'Textiles & Apparel'
            WHEN SUBSTRING(matkl, 1, 2) IN ('20', '28', '29', '30', '39', '40') AND SUBSTRING(matkl, 3, 1) = '2' THEN 'Paper, Printing & Packaging (Pic)'
            WHEN matkl = '203010' THEN 'R&D / Sample Development'
            WHEN matkl = '283200' THEN 'Textiles & Apparel'
            WHEN matkl LIKE '293%' THEN 'Corporate & Logistics Services'
            WHEN matkl LIKE '37%' THEN 'Packaging'
            WHEN matkl LIKE '50%' THEN 'Plant Maintenance & Engineering'
            WHEN SUBSTRING(matkl, 1, 4) IN ('6002', '6003', '6004', '6005') THEN 'Textiles & Apparel'
            WHEN SUBSTRING(matkl, 1, 4) = '6007' THEN 'Paper, Printing & Packaging (Pic)'
            WHEN SUBSTRING(matkl, 1, 3) BETWEEN '600' AND '607' THEN 'General Factory & Admin Consumables'
            WHEN SUBSTRING(matkl, 1, 3) BETWEEN '608' AND '609' THEN 'Capital Assets'
            WHEN SUBSTRING(matkl, 1, 3) BETWEEN '700' AND '707' THEN 'General Admin & Plant Operations'
            WHEN SUBSTRING(matkl, 1, 3) BETWEEN '708' AND '709' THEN 'Capital Assets (CWIP)'
            WHEN matkl LIKE 'ESN-%' THEN 'Finance & General Admin (GL Mapped)'
            WHEN matkl LIKE 'RMA3%' THEN 'Textiles & Apparel'
            WHEN matkl LIKE 'RMMG-%' OR matkl LIKE 'TRMG-%' OR matkl IN ('FGCVL01', 'FGREL-PRJ') THEN 'Real Estate & Construction'
            WHEN matkl LIKE '%SM01' THEN 'Healthcare / PPE (Surgical Masks)'
            WHEN matkl LIKE '%AG01' THEN 'Agro Business'
            WHEN matkl LIKE '%PF0_' THEN 'Food & Confectionery (Pic Food)'
            WHEN matkl IN ('FGCT01', 'SFCT01') THEN 'Hygiene & Medical (Cotton Wool)'
            WHEN matkl ~ '^(FG|SF)(EN|MC|PB|PN|PP|TP)01$' THEN 'Paper, Printing & Packaging (Pic)'
            ELSE 'General / Unassigned'
        END AS level_2_division,

        -- LEVEL 3: Process Stage / Sub-Category
        CASE 
            WHEN matkl = '110100' THEN 'Unclassified Trading Good'
            WHEN matkl = '110200' THEN 'Paper Products'
            WHEN matkl = '110300' THEN 'Food Stuff'
            WHEN matkl = '110400' THEN 'Toiletries'
            WHEN matkl = '110500' THEN 'Hardware'
            WHEN matkl = '110600' THEN 'Construction Material'
            WHEN matkl = '110700' THEN 'Household'
            WHEN matkl = '119900' THEN 'Miscellaneous Trading'
            WHEN matkl LIKE '2011%' THEN 'Yarn'
            WHEN matkl LIKE '2012%' THEN 'Greige Fabric'
            WHEN matkl LIKE '2013%' THEN 'Finished Fabric (Dyed/Printed)'
            WHEN matkl LIKE '2014%' THEN 'Made-Ups (Home Textiles)'
            WHEN matkl LIKE '2015%' THEN 'Garments'
            WHEN matkl LIKE '2021%' THEN 'Exercise & Other Books'
            WHEN matkl LIKE '2022%' THEN 'Bound Books (Counter/Spiral/Printed)'
            WHEN matkl LIKE '2023%' THEN 'Commercial & Security Printing'
            WHEN matkl LIKE '2024%' THEN 'Computer Stationery (Single/Multi-ply)'
            WHEN matkl LIKE '2025%' THEN 'Paper Reams'
            WHEN matkl LIKE '2026%' THEN 'Published Textbooks'
            WHEN matkl = '202700'   THEN 'Power & Energy'
            WHEN matkl LIKE '2029%' THEN 'Specialty Rolls, Tapes & Tubes'
            WHEN matkl = '203010'   THEN 'Sample Development'
            WHEN matkl IN ('281200', '401200', '600200') OR matkl LIKE 'RMA30%' THEN 'Spinning (Spg)'
            WHEN matkl IN ('281300', '391300', '401300', '600300') OR matkl LIKE 'RMA31%' THEN 'Weaving (Wvg)'
            WHEN matkl IN ('281400', '291400', '391400', '401400', '600400') OR matkl LIKE 'RMA32%' THEN 'Processing & Dyeing (Proc)'
            WHEN matkl IN ('281500', '291500', '391500', '401500', '600500') OR matkl LIKE 'RMA33%' THEN 'Garments (Gmt)'
            WHEN matkl LIKE 'RMA34%' THEN 'Textile Packing (PK)'
            WHEN matkl IN ('281100', '283200') THEN 'General Textile Waste'
            WHEN matkl IN ('282000', '292000', '402000', '600700') OR matkl LIKE '3920%' THEN 'Paper & Printing Operations (Pic)'
            WHEN matkl LIKE '3011%' THEN 'Fibre'
            WHEN matkl LIKE '3012%' THEN 'Yarn'
            WHEN matkl LIKE '3013%' THEN 'Fabric (Greige/Finished)'
            WHEN matkl LIKE '3014%' THEN 'Dyes & Pigments'
            WHEN matkl LIKE '3015%' THEN 'Textile Chemicals'
            WHEN matkl LIKE '3016%' THEN 'Auxiliaries'
            WHEN matkl LIKE '3017%' THEN 'Garment Accessories'
            WHEN SUBSTRING(matkl, 1, 5) IN ('30219', '30220', '30221') AND matkl <> '302191' THEN 'Flexible Packaging Films & Foils (Fx)'
            WHEN matkl LIKE '3021%' THEN 'Paper & Paperboard'
            WHEN matkl = '302290'   THEN 'Binding Materials'
            WHEN matkl LIKE '3023%' THEN 'Printing Inks'
            WHEN matkl = '302400'   THEN 'Power & Energy'
            WHEN matkl LIKE '3700%' THEN 'Packaging Materials'
            WHEN matkl LIKE '501%' THEN 'Instrumentation Spares'
            WHEN matkl LIKE '502%' THEN 'Mechanical Spares'
            WHEN matkl LIKE '503%' THEN 'Automotive Spares'
            WHEN matkl LIKE '504%' THEN 'Electrical & Electronic Spares'
            WHEN matkl LIKE '505%' THEN 'Non-Consumable Tools'
            WHEN matkl IN ('600800', '600900', '604000', '605000') THEN 'Fuels, Oils & Lubricants'
            WHEN matkl LIKE '601%' THEN 'Hardware, Piping, Tools & Lab Supplies'
            WHEN matkl LIKE '6020%' THEN 'Staff Welfare, Uniforms & Canteen'
            WHEN matkl = '602100' THEN 'IT Equipment'
            WHEN matkl BETWEEN '603000' AND '607999' THEN 'Safety, Stationery, Janitorial & Misc'
            WHEN SUBSTRING(matkl, 1, 3) IN ('608', '609', '708', '709') THEN 'Land, Buildings, Plant & Equipment'
            WHEN matkl LIKE '701%' THEN 'Repairs & Maintenance (R&M)'
            WHEN matkl LIKE '704%' AND SUBSTRING(matkl, 4, 1) BETWEEN '5' AND '9' THEN 'Insurance'
            WHEN matkl = '705000' THEN 'Insurance'
            WHEN SUBSTRING(matkl, 1, 3) BETWEEN '700' AND '707' THEN 'Utilities, Rent, Consulting & Admin'
            WHEN matkl LIKE 'ESN-%' THEN 'Direct GL Expense Services'
            WHEN matkl LIKE 'RMMG-%' OR matkl LIKE 'TRMG-%' OR matkl IN ('FGCVL01', 'FGREL-PRJ') THEN 'Construction & Civil Supplies'
            WHEN matkl LIKE '%SM01' THEN 'Surgical Masks'
            WHEN matkl LIKE '%AG01' THEN 'Agro Products'
            WHEN matkl LIKE '%PF0_' THEN 'Confectionery (Candy/Gum/Lollypop)'
            ELSE clean_item_desc
        END AS level_3_subcategory
    FROM cleaned_t023t
)

SELECT
    -- ORGANIZATIONAL KEY
    t001k.BUKRS                                               AS COMPANY_CODE,
    marc.WERKS                                                AS BRANCH_ID,
    mara.MATNR                                                AS PRODUCT_CODE,

    -- PRODUCT
    mara.SIZE1                                                AS "SIZE",
    makt.MAKTX                                                AS PRODUCT,

    -- UNIT / WEIGHT
    mara.MEINS                                                AS UNIT_CODE,
    t006a.MSEHL                                               AS UNIT,
    mara.GEWEI                                                AS WEIGHT_UNIT,
    NULLIF(TRIM(mara.NTGEW::text), '')::numeric               AS WEIGHT_PCS,

    -- MATERIAL ATTRIBUTES
    mara.ERSDA                                                AS CREATION_DATE,
    mara.BRAND_ID                                             AS BRAND_CODE,

    -- MRP / STOCK PARAMETERS
    NULLIF(TRIM(marc.MINBE::text), '')::numeric               AS REORDER_LEVEL,
    NULLIF(TRIM(marc.MINBE::text), '')::numeric               AS MIN_LEVEL,
    NULLIF(TRIM(marc.MABST::text), '')::numeric               AS MAX_LEVEL,
    NULLIF(TRIM(marc.EISBE::text), '')::numeric               AS SAFETY_STOCK,
    NULLIF(TRIM(marc.EISLO::text), '')::numeric               AS MINIMUM_SAFETY_STOCK,
    NULLIF(TRIM(marc.WZEIT::text), '')::numeric               AS LEAD_TIME_IN_DAYS,
    marc.MAABC                                                AS ABC,

    -- VALUATION / PRICE
    mbew.VPRSV                                                AS PRICE_CONTROL,
    NULLIF(TRIM(mbew.STPRS::text), '')::numeric               AS STANDARD_PRICE,
    NULLIF(TRIM(mbew.VERPR::text), '')::numeric               AS MOVING_AVG_PRICE,
    NULLIF(TRIM(mbew.PEINH::text), '')::numeric               AS PRICE_UNIT,

    -- Effective price per PEINH units
    CASE
        WHEN mbew.VPRSV = 'V'
            THEN NULLIF(TRIM(mbew.VERPR::text), '')::numeric
        ELSE NULLIF(TRIM(mbew.STPRS::text), '')::numeric
    END                                                       AS PRICE,

    -- UNIT PRICE - RAW
    ROUND(
        (
            CASE
                WHEN mbew.VPRSV = 'V'
                    THEN NULLIF(TRIM(mbew.VERPR::text), '')::numeric
                ELSE NULLIF(TRIM(mbew.STPRS::text), '')::numeric
            END
        )
        / NULLIF(NULLIF(TRIM(mbew.PEINH::text), '')::numeric, 0),
        4
    )                                                         AS UNIT_PRICE_RAW,

    -- CURRENCY
    t001.WAERS                                                AS CURRENCY,
    COALESCE(NULLIF(TRIM(tcurx.CURRDEC::text), '')::int, 2)   AS CURRDEC_APPLIED,

    -- CURRENCY-ADJUSTED UNIT PRICE
    ROUND(
        (
            CASE
                WHEN mbew.VPRSV = 'V'
                    THEN NULLIF(TRIM(mbew.VERPR::text), '')::numeric
                ELSE NULLIF(TRIM(mbew.STPRS::text), '')::numeric
            END
        )
        / NULLIF(NULLIF(TRIM(mbew.PEINH::text), '')::numeric, 0)
        * POWER(
            10,
            2 - COALESCE(NULLIF(TRIM(tcurx.CURRDEC::text), '')::int, 2)
        )::numeric,
        2
    )                                                         AS UNIT_PRICE_ADJUSTED,

    -- MATERIAL GROUP HIERARCHY (T023T)
    TRIM(mara.MATKL)                                          AS MATERIAL_GROUP_CODE,
    COALESCE(mh.level_1_category, 'Unmapped Material Group')  AS LEVEL_1_CATEGORY,
    COALESCE(mh.level_2_division, 'Unmapped Division')        AS LEVEL_2_DIVISION,
    COALESCE(mh.level_3_subcategory, 'Unmapped Sub-Category') AS LEVEL_3_SUBCATEGORY,
    mh.clean_item_desc                                        AS MATERIAL_GROUP_DESC,
    mh.raw_full_desc                                          AS SAP_MATKL_FULL_DESC,

    -- MATERIAL TYPE (T134T)
    TRIM(t134t.MTART)                                         AS "Material Type",
    TRIM(t134t.MTBEZ)                                         AS "Material Type Description"

FROM sap_stg_ecc."stg_sap_MM_mara" mara

INNER JOIN sap_stg_ecc."stg_sap_MM_marc" marc
    ON mara.MATNR = marc.MATNR

LEFT JOIN sap_stg_ecc."stg_sap_MM_makt" makt
    ON mara.MATNR = makt.MATNR
    AND TRIM(makt.SPRAS) = 'E'

LEFT JOIN sap_stg_ecc."stg_sap_MM_t006a" t006a
    ON mara.MEINS = t006a.MSEHI
    AND TRIM(t006a.SPRAS) = 'E'

LEFT JOIN sap_stg_ecc."stg_sap_MM_t001w" t001w
    ON marc.WERKS = t001w.WERKS

LEFT JOIN sap_stg_ecc."stg_sap_MM_t001k" t001k
    ON t001w.BWKEY = t001k.BWKEY

-- ── MATERIAL GROUP HIERARCHY JOIN ────────────────────────────────
LEFT JOIN material_group_hierarchy mh
    ON TRIM(mara.MATKL) = mh.matkl

-- ── MATERIAL TYPE (CODE + DESCRIPTION) ───────────────────────────
LEFT JOIN sap_stg_ecc."stg_sap_MM_t134t" t134t
    ON TRIM(mara.MTART) = TRIM(t134t.MTART)
    AND TRIM(t134t.SPRAS) = 'E'

-- ── VALUATION / PRICE ────────────────────────────────────────────
LEFT JOIN sap_stg_ecc."stg_sap_MM_mbew" mbew
    ON mara.MATNR = mbew.MATNR
    AND t001w.BWKEY = mbew.BWKEY
    AND COALESCE(TRIM(mbew.BWTAR), '') = ''

-- ── COMPANY CURRENCY ─────────────────────────────────────────────
LEFT JOIN sap_stg_ecc."stg_sap_MM_t001" t001
    ON t001k.BUKRS = t001.BUKRS

-- ── CURRENCY DECIMAL PLACES ──────────────────────────────────────
LEFT JOIN sap_stg_ecc."stg_sap_MM_tcurx" tcurx
    ON t001.WAERS = tcurx.CURRKEY

-- ── COMPANY CODE FILTER ──────────────────────────────────────────
WHERE COALESCE(TRIM(t001k.BUKRS), '') IN ('1200', '1400', '2600', '2700', 'DAPL', 'HECL', 'HPDL', 'PFL')

ORDER BY 
    t001k.BUKRS,
    marc.WERKS,
    mara.MATKL,
    mara.MATNR;
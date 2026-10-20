SELECT 
    -- LEVEL 1: MAIN GROUP
    pg_level1.PRGRP AS MAIN_GROUP,
    makt_main.MAKTX AS MAIN_PGROUP_DESCRIPTION,

    -- LEVEL 2: PRODUCT GROUP
    pg_level1.NRMIT AS PRODUCT_GROUP,
    makt_sub.MAKTX AS PGROUP_DESCRIPTION,

    -- LEVEL 3: MATERIAL
    pg_level2.NRMIT AS MATERIAL,
    makt_mat.MAKTX AS MATERIAL_DESCRIPTION,

    -- MATERIAL DETAILS
    mara.MTART AS MATERIAL_TYPE,
    t134t.MTBEZ AS MATERIAL_TYPE_DESCRIPTION,

    mara.MATKL AS MATERIAL_GROUP,
    t023t.WGBEZ AS MATERIAL_GROUP_DESCRIPTION,

    mara.MEINS AS UOM,

    -- PLANT + STORAGE LOCATION
    mard.WERKS AS PLANT,
    mard.LGORT AS STORAGE_LOCATION

FROM SAP_PGMI_DATA pg_level1

INNER JOIN SAP_PGMI_DATA pg_level2
    ON pg_level1.NRMIT = pg_level2.PRGRP


-- Main Group Description
LEFT JOIN SAP_MAKT_DATA makt_main
    ON pg_level1.PRGRP = makt_main.MATNR
    AND makt_main.SPRAS = 'E'


-- Product Group Description
LEFT JOIN SAP_MAKT_DATA makt_sub
    ON pg_level1.NRMIT = makt_sub.MATNR
    AND makt_sub.SPRAS = 'E'


-- Material Description
LEFT JOIN SAP_MAKT_DATA makt_mat
    ON pg_level2.NRMIT = makt_mat.MATNR
    AND makt_mat.SPRAS = 'E'


-- Material Master
LEFT JOIN SAP_MARA_DATA mara
    ON pg_level2.NRMIT = mara.MATNR


-- Material Type Description
LEFT JOIN SAP_T134T_DATA t134t
    ON mara.MTART = t134t.MTART
    AND t134t.SPRAS = 'E'


-- Material Group Description
LEFT JOIN SAP_T023T_DATA t023t
    ON mara.MATKL = t023t.MATKL
    AND t023t.SPRAS = 'E'


-- Plant + Storage Location
LEFT JOIN SAP_MARD_DATA mard
    ON pg_level2.NRMIT = mard.MATNR
;
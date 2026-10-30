
SELECT
    -- Purchase order and material details
    s2.ebeln             AS "PO NUMBER",
    h.aedat              AS "PO DATE",
    req.afnam            AS "REQUISITIONER",
    s2.poitemseq         AS "SEQ.NO",
    s2.matnr             AS "MATERIAL",
    s2.maktx             AS "DESCRIPTION",

    NULLIF(TRIM(s2.menge), '')::NUMERIC
                           AS "PO QTY.",

    s2.meins             AS "UOM",

    NULLIF(TRIM(s2.weight), '')::NUMERIC
                           AS "WEIGHT",

    s2.wt_uom            AS "WEIGHT UOM",

    NULLIF(TRIM(s2.volum), '')::NUMERIC
                           AS "VOLUME",

    s2.vol_uom           AS "VOLUME UOM",
    s2.inv_no            AS "INV NO.",
    s2.inv_date          AS "INV.DATE",

    -- Container details
    s1.container         AS "CONTAINER NO",
    s1.cont_type         AS "TYP",
    s1.cont_size         AS "SIZE",

    NULLIF(TRIM(s1.container_wt), '')::NUMERIC
                           AS "CONTAINER WEIGHT",

    s1.wt_unit           AS "CONTAINER WEIGHT UOM",

    NULLIF(TRIM(s1.container_vol), '')::NUMERIC
                           AS "CONTAINER VOLUME",

    s1.vol_unit          AS "CONTAINER VOLUME UOM",

    -- Shipping details
    s1.bill_of_lad       AS "BILL OF LADING",
    s1.bl_date           AS "B/L DATE",
    s1.vessel_no         AS "VESSEL NO.",
    s1.voyage_no         AS "VOYAGE NO.",
    s1.shipment_no       AS "SHIPMENT NO",
    s1.date_of_shipment  AS "SHIPMENT DATE",
    s1.shipment_typ      AS "SHIPMENT TYPE",
    s1.shipping_line     AS "SHIPPING LINE",
    s1.smode             AS "MODE",
    s1.from_port         AS "FROM PORT",
    s1.to_port           AS "TO PORT",

    -- Port arrival and trucking
    s1.eda_port          AS "EDA@PORT",
    s1.date_of_port      AS "ACT. ARIVAL",
    s1.arrival_no1       AS "PORT AVRL.NO.",
    s1.ed_trucking       AS "ED TRUCKING",
    s1.act_trucking_dt   AS "ACT.TRUCKING",
    s1.truck_no          AS "TRUCK NO.",
    s1.transporter_nm    AS "TRANSPORTER NAME",
    s1.consignment_no    AS "CONSIG.No.",
    s1.consignment_dt    AS "CONSIG.DATE",

    -- Factory arrival and document tracking
    s1.eda_fact          AS "EDA@FACT",
    s1.act_fact_dt       AS "ACT.FACT.DATE",
    s1.arrival_no2       AS "FACT ARVL.NO.",
    s1.gatepass_no       AS "GATEPASS NO",
    s1.doc_rcvd          AS "DOC RCVD.",
    s1.doc_rcvd_dt       AS "DOC RCVD.DT"

FROM sap_stg_ecc.stg_sap_git_zshipment02 AS s2

INNER JOIN sap_stg_ecc.stg_sap_git_zshipment01 AS s1
    ON s1.container = s2.container

INNER JOIN sap_stg_ecc.stg_sap_git_ekko AS h
    ON h.ebeln = s2.ebeln

-- Reproduce the ABAP's conditional SELECT SINGLE from EKPO
LEFT JOIN LATERAL (
    SELECT ep.afnam
    FROM sap_stg_ecc.stg_sap_git_ekpo AS ep
    WHERE ep.ebeln = s2.ebeln
      AND (
            (
                COALESCE(s2.matnr, '') <> ''
                AND ep.matnr = s2.matnr
            )
            OR
            (
                COALESCE(s2.matnr, '') = ''
                AND ep.ebelp = s2.poitemseq
            )
          )
    LIMIT 1
) AS req ON TRUE

WHERE s2.active2 = 'X'

-- Optional date filter
-- AND h.aedat >= '20260101'
-- AND h.aedat <  '20270101'

-- Optional filters
-- AND s2.comp = 'YOUR_COMPANY_CODE'
-- AND s2.ebeln = 'YOUR_PO_NUMBER'
-- AND h.aedat BETWEEN '20260101' AND '20261231'
-- AND s2.matnr = 'YOUR_MATERIAL'
-- AND s2.inv_no = 'YOUR_INVOICE_NUMBER'

ORDER BY
    s2.ebeln,
    s2.poitemseq,
    s2.matnr;

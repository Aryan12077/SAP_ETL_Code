WITH CTE_GR_QTY AS (
        SELECT
            ebeln,
            ebelp,
            SUM(
                CASE shkzg
                    WHEN 'H'
                    THEN CAST(NULLIF(menge, '') AS NUMERIC(18,3)) * -1
                    ELSE CAST(NULLIF(menge, '') AS NUMERIC(18,3))
                END
            ) AS gr_qty,

            MAX(
                TO_DATE(NULLIF(budat, ''), 'YYYYMMDD')
            ) AS grn_date

        FROM "sap_stg_ecc"."stg_sap_me2m_ekbe"

        WHERE bewtp = 'E'

        GROUP BY
            ebeln,
            ebelp
    ),

    CTE_IR_QTY AS (
        SELECT
            ebeln,
            ebelp,

            SUM(
                CASE shkzg
                    WHEN 'H'
                    THEN CAST(NULLIF(menge, '') AS NUMERIC(18,3)) * -1
                    ELSE CAST(NULLIF(menge, '') AS NUMERIC(18,3))
                END
            ) AS ir_qty

        FROM "sap_stg_ecc"."stg_sap_me2m_ekbe"

        WHERE bewtp = 'Q'

        GROUP BY
            ebeln,
            ebelp
    ),

    CTE_PO_HIST_FLAG AS (
        SELECT DISTINCT
            ebeln,
            ebelp,
            'X' AS po_hist_flag

        FROM "sap_stg_ecc"."stg_sap_me2m_ekbe"
    ),

    CTE_PO_POSITIONS AS (
        SELECT
            ebeln,
            COUNT(DISTINCT ebelp) AS no_of_positions

        FROM "sap_stg_ecc"."stg_sap_me2m_ekpo"

        WHERE COALESCE(loekz, '') = ''

        GROUP BY ebeln
    ),

    CTE_EINDT AS (
        SELECT
            ebeln,
            ebelp,

            MIN(
                TO_DATE(NULLIF(eindt, ''), 'YYYYMMDD')
            ) AS expected_delivery_date

        FROM "sap_stg_ecc"."stg_sap_me2m_eket"

        GROUP BY
            ebeln,
            ebelp
    )

    SELECT

        hd.ebeln AS "Purchasing Document",

        po.ebelp AS "Item",

        hd.bsart AS "Purchasing Doc. Type",

        hd.bstyp AS "Purch. Doc. Category",

        hd.ekgrp AS "Purchasing Group",

        COALESCE(ph.po_hist_flag, '')
            AS "PO history/release documentation",

        TO_DATE(
            NULLIF(hd.bedat, ''),
            'YYYYMMDD'
        ) AS "Purchase Order Date",

        ed.expected_delivery_date
            AS "Expected Delivery Date",

        gr.grn_date
            AS "GRN Date",

        hd.lifnr || '     ' ||
        COALESCE(vn.name1, '')
            AS "Vendor/supplying plant",

        po.matnr
            AS "Material",

        COALESCE(
            NULLIF(mk.maktx, ''),
            NULLIF(po.txz01, ''),
            '— no description —'
        ) AS "Material Description",

        po.matkl
            AS "Material Group",

        COALESCE(po.loekz, '')
            AS "Deletion Indicator",

        po.pstyp
            AS "Item Category",

        po.knttp
            AS "Acct Assignment Cat.",

        po.werks
            AS "Plant",

        hd.bukrs
            AS "Company Name",

        po.lgort
            AS "Storage Location",

        CAST(NULLIF(po.menge, '') AS NUMERIC(18,3))
            AS "Order Quantity",

        po.meins
            AS "Order Unit",

        ROUND(
            CAST(NULLIF(po.menge, '') AS NUMERIC(18,3))
            *
            (
                CAST(NULLIF(po.umrez, '') AS NUMERIC(18,6))
                /
                NULLIF(
                    CAST(NULLIF(po.umren, '') AS NUMERIC(18,6)),
                    0
                )
            ),
            3
        ) AS "Quantity in SKU",

        po.lmein
            AS "Stockkeeping unit",

        CASE
            WHEN hd.waers IN ('UGX', 'RWF')
            THEN CAST(NULLIF(po.netpr, '') AS NUMERIC(18,2)) * 100
            ELSE CAST(NULLIF(po.netpr, '') AS NUMERIC(18,2))
        END AS "Net price",

        hd.waers
            AS "Currency",

        CAST(NULLIF(po.peinh, '') AS NUMERIC(18,3))
            AS "Price Unit",

        CAST(NULLIF(po.ktmng, '') AS NUMERIC(18,3))
            AS "Target Quantity",

        ROUND(
            COALESCE(
                CAST(NULLIF(po.ktmng, '') AS NUMERIC(18,3)),
                0
            )
            -
            COALESCE(gr.gr_qty, 0),
            3
        ) AS "Open Target Quantity",

        ROUND(
            COALESCE(
                CAST(NULLIF(po.menge, '') AS NUMERIC(18,3)),
                0
            )
            -
            COALESCE(gr.gr_qty, 0),
            3
        ) AS "Still to be delivered (qty)",

        ROUND(
            (
                COALESCE(
                    CAST(NULLIF(po.menge, '') AS NUMERIC(18,3)),
                    0
                )
                -
                COALESCE(gr.gr_qty, 0)
            )
            *
            (
                CASE
                    WHEN hd.waers IN ('UGX', 'RWF')
                    THEN CAST(NULLIF(po.netpr, '') AS NUMERIC(18,2)) * 100
                    ELSE CAST(NULLIF(po.netpr, '') AS NUMERIC(18,2))
                END
                /
                NULLIF(
                    CAST(NULLIF(po.peinh, '') AS NUMERIC(18,3)),
                    0
                )
            ),
            2
        ) AS "Still to be delivered (value)",

        ROUND(
            COALESCE(
                CAST(NULLIF(po.menge, '') AS NUMERIC(18,3)),
                0
            )
            -
            COALESCE(ir.ir_qty, 0),
            3
        ) AS "Still to be invoiced (qty)",

        COALESCE(gr.gr_qty, 0)
            AS "Qty Released to Date",

        ROUND(
            (
                COALESCE(
                    CAST(NULLIF(po.menge, '') AS NUMERIC(18,3)),
                    0
                )
                -
                COALESCE(ir.ir_qty, 0)
            )
            *
            (
                CAST(NULLIF(po.netpr, '') AS NUMERIC(18,2))
                /
                NULLIF(
                    CAST(NULLIF(po.peinh, '') AS NUMERIC(18,3)),
                    0
                )
            ),
            2
        ) AS "Still to be invoiced (val.)",

        COALESCE(pos.no_of_positions, 0)
            AS "No. of Positions",

        hd.frgzu
            AS "Release status",

        hd.frgsx
            AS "Release Strategy",

        hd.frggr
            AS "Release group"

    FROM "sap_stg_ecc"."stg_sap_me2m_ekpo" po

    INNER JOIN "sap_stg_ecc"."stg_sap_me2m_ekko" hd
        ON hd.ebeln = po.ebeln

    LEFT JOIN CTE_GR_QTY gr
        ON gr.ebeln = po.ebeln
       AND gr.ebelp = po.ebelp

    LEFT JOIN CTE_IR_QTY ir
        ON ir.ebeln = po.ebeln
       AND ir.ebelp = po.ebelp

    LEFT JOIN CTE_PO_HIST_FLAG ph
        ON ph.ebeln = po.ebeln
       AND ph.ebelp = po.ebelp

    LEFT JOIN CTE_PO_POSITIONS pos
        ON pos.ebeln = po.ebeln

    LEFT JOIN CTE_EINDT ed
        ON ed.ebeln = po.ebeln
       AND ed.ebelp = po.ebelp

    LEFT JOIN "sap_stg_ecc"."stg_sap_me2m_lfa1" vn
        ON vn.lifnr = hd.lifnr

    LEFT JOIN "sap_stg_ecc"."stg_sap_me2m_makt" mk
        ON mk.matnr = po.matnr

    WHERE COALESCE(po.loekz, '') = ''

    ORDER BY
        hd.bedat ASC,
        hd.ebeln ASC,
        po.ebelp ASC;
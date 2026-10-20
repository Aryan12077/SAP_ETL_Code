WITH CTE_EBKN_PRIMARY AS
    (
        SELECT DISTINCT ON (banfn, bnfpo)
            banfn,
            bnfpo,
            sakto,
            aufnr,
            kostl,
            gsber,
            prctr,
            ps_psp_pnr,
            wempf,
            ablad
        FROM sap_stg_ecc.stg_sap_prstatus_ebkn
        WHERE COALESCE(loekz, '') = ''
        ORDER BY banfn, bnfpo, zebkn ASC
    ),

    CTE_GR AS
    (
        SELECT
            ebeln,
            ebelp,

            MIN(
                CASE
                    WHEN shkzg = 'S'
                    THEN TO_DATE(NULLIF(budat, ''), 'YYYYMMDD')
                END
            ) AS fst_grn_dt,

            SUM(
                CASE shkzg
                    WHEN 'H'
                    THEN CAST(NULLIF(menge, '') AS NUMERIC(18,3)) * -1
                    ELSE CAST(NULLIF(menge, '') AS NUMERIC(18,3))
                END
            ) AS total_grn_qty

        FROM sap_stg_ecc.stg_sap_prstatus_ekbe
        WHERE vgabe = '1'
        GROUP BY ebeln, ebelp
    ),

    CTE_IR AS
    (
        SELECT
            ebeln,
            ebelp,

            MIN(
                CASE
                    WHEN shkzg = 'S'
                    THEN TO_DATE(NULLIF(budat, ''), 'YYYYMMDD')
                END
            ) AS fst_inv_dt,

            SUM(
                CASE shkzg
                    WHEN 'H'
                    THEN CAST(NULLIF(menge, '') AS NUMERIC(18,3)) * -1
                    ELSE CAST(NULLIF(menge, '') AS NUMERIC(18,3))
                END
            ) AS total_inv_qty,

            SUM(
                CASE shkzg
                    WHEN 'H'
                    THEN CAST(NULLIF(dmbtr, '') AS NUMERIC(18,2)) * -1
                    ELSE CAST(NULLIF(dmbtr, '') AS NUMERIC(18,2))
                END
            ) AS ttl_inv_val_booked

        FROM sap_stg_ecc.stg_sap_prstatus_ekbe
        WHERE vgabe = '2'
        GROUP BY ebeln, ebelp
    ),

    CTE_LAST_GR_MVT AS
    (
        SELECT DISTINCT ON (ebeln, ebelp)
            ebeln,
            ebelp,
            bwart,
            vgabe
        FROM sap_stg_ecc.stg_sap_prstatus_ekbe
        WHERE vgabe = '1'
        ORDER BY
            ebeln,
            ebelp,
            TO_DATE(NULLIF(budat, ''), 'YYYYMMDD') DESC NULLS LAST,
            belnr DESC
    )

    SELECT

        -- 1. PUR.REQ.NO
        pr.banfn AS "PUR.REQ.NO",

        -- 2. ITEM
        pr.bnfpo AS "ITEM",

        -- 3. MAT_CODE
        pr.matnr AS "MAT_CODE",

        -- 4. DESCRIPTION
        pr.txz01 AS "DESCRIPTION",

        -- 5. REQ.DATE
        TO_DATE(NULLIF(pr.badat, ''), 'YYYYMMDD')
            AS "REQ.DATE",

        -- 6. DEL.DATE
        TO_DATE(NULLIF(pr.lfdat, ''), 'YYYYMMDD')
            AS "DEL.DATE",

        -- 7. PUR.GROUP
        pr.ekgrp AS "PUR.GROUP",

        -- 8. QUANTITY
        CAST(NULLIF(pr.menge, '') AS NUMERIC(18,3))
            AS "QUANTITY",

        -- 9. UOM
        pr.meins AS "UOM",

        -- 10. REL.STATUS
        pr.frgzu AS "REL.STATUS",

        -- 11. REL.IND
        pr.frgkz AS "REL.IND",

        -- 12. CLOSED
        pr.ebakz AS "CLOSED",

        -- 13. REQUISITIONER
        pr.afnam AS "REQUISITIONER",

        -- 14. TRACKING NO.
        pr.bednr AS "TRACKING NO.",

        -- 15. CREATED BY
        pr.ernam AS "CREATED BY",

        -- 16. PLANT
        pr.werks AS "PLANT",

        -- 17. G/L ACCOUNT
        COALESCE(ac.sakto, '')
            AS "G/L ACCOUNT",

        -- 18. G/L ACCOUNT DESC.
        COALESCE(gl.txt50, '')
            AS "G/L ACCOUNT DESC.",

        -- 19. INT.ORDER
        COALESCE(ac.aufnr, '')
            AS "INT.ORDER",

        -- 20. INT.ORDER DESC.
        COALESCE(io.ktext, '')
            AS "INT.ORDER DESC.",

        -- 21. PO/RFQ NO.
        COALESCE(po.ebeln, '')
            AS "PO/RFQ NO.",

        -- 22. PO DATE
        TO_DATE(NULLIF(hd.bedat, ''), 'YYYYMMDD')
            AS "PO DATE",

        -- 23. VENDOR (CODE)
        COALESCE(hd.lifnr, '')
            AS "VENDOR (CODE)",

        -- 24. VENDOR NAME
        COALESCE(vn.name1, '')
            AS "VENDOR NAME",

        -- 25. CURRENCY
        COALESCE(hd.waers, pr.waers, '')
            AS "CURRENCY",

        -- 26. TOTAL PO VAL
        CAST(NULLIF(po.netwr, '') AS NUMERIC(18,2))
            AS "TOTAL PO VAL",

        -- 27. PO STATUS
        COALESCE(po.loekz, '')
            AS "PO STATUS",

        -- 28. FST GRN.DT
        gr.fst_grn_dt
            AS "FST GRN.DT",

        -- 29. TOTAL GRN QT
        COALESCE(gr.total_grn_qty, 0)
            AS "TOTAL GRN QT",

        -- 30. FST INV.DT
        ir.fst_inv_dt
            AS "FST INV.DT",

        -- 31. TOTAL INV QT
        COALESCE(ir.total_inv_qty, 0)
            AS "TOTAL INV QT",

        -- 32. TTL INV_VAL_BOOKED
        COALESCE(ir.ttl_inv_val_booked, 0)
            AS "TTL INV_VAL_BOOKED",

        -- 33. MOVEMENT TYPE
        mv.bwart
            AS "MOVEMENT TYPE",

        -- 34. TRANSACTION TYPE
        mv.vgabe
            AS "TRANSACTION TYPE"

    FROM sap_stg_ecc.stg_sap_prstatus_eban pr

    -- Primary account assignment
    LEFT JOIN CTE_EBKN_PRIMARY ac
        ON ac.banfn = pr.banfn
       AND ac.bnfpo = pr.bnfpo

    -- Internal order description
    LEFT JOIN sap_stg_ecc.stg_sap_prstatus_aufk io
        ON io.aufnr = ac.aufnr
       AND COALESCE(ac.aufnr, '') <> ''

    -- G/L account description
    LEFT JOIN sap_stg_ecc.stg_sap_prstatus_skat gl
        ON gl.saknr = ac.sakto
       AND COALESCE(ac.sakto, '') <> ''

    -- PO items referencing the PR item
    LEFT JOIN sap_stg_ecc.stg_sap_prstatus_ekpo po
        ON po.banfn = pr.banfn
       AND po.bnfpo = pr.bnfpo

    -- PO header
    LEFT JOIN sap_stg_ecc.stg_sap_prstatus_ekko hd
        ON hd.ebeln = po.ebeln

    -- Vendor master
    LEFT JOIN sap_stg_ecc.stg_sap_prstatus_lfa1 vn
        ON vn.lifnr = hd.lifnr

    -- Goods receipt aggregates
    LEFT JOIN CTE_GR gr
        ON gr.ebeln = po.ebeln
       AND gr.ebelp = po.ebelp

    -- Invoice receipt aggregates
    LEFT JOIN CTE_IR ir
        ON ir.ebeln = po.ebeln
       AND ir.ebelp = po.ebelp

    -- Latest GR movement type
    LEFT JOIN CTE_LAST_GR_MVT mv
        ON mv.ebeln = po.ebeln
       AND mv.ebelp = po.ebelp

    ORDER BY
        pr.badat ASC,
        pr.banfn ASC,
        pr.bnfpo ASC,
        po.ebeln ASC NULLS LAST;
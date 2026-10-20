/* ============================================================
    MB51 — Material Document List | PostgreSQL
   ============================================================ */

WITH CTE_AMOUNT AS (
    SELECT
        mblnr,
        mjahr,
        zeile,
        CASE shkzg
            WHEN 'H' THEN CAST(NULLIF(dmbtr, '') AS NUMERIC(18,2)) * -1
            ELSE           CAST(NULLIF(dmbtr, '') AS NUMERIC(18,2))
        END                                         AS amount_signed,
        CASE shkzg
            WHEN 'H' THEN CAST(NULLIF(erfmg, '') AS NUMERIC(18,3)) * -1
            ELSE           CAST(NULLIF(erfmg, '') AS NUMERIC(18,3))
        END                                         AS qty_signed,
        CASE shkzg
            WHEN 'H' THEN CAST(NULLIF(menge, '') AS NUMERIC(18,3)) * -1
            ELSE           CAST(NULLIF(menge, '') AS NUMERIC(18,3))
        END                                         AS qty_base_signed
    FROM "sap_stg_ecc"."stg_sap_mb51_mseg"
),

CTE_MVMT_CLASS AS (
    SELECT
        mblnr,
        mjahr,
        zeile,
        bwart,
        CASE
            WHEN bwart IN ('101','105','501','503','505','551','561') THEN 'Goods Receipt'
            WHEN bwart IN ('102','106','502','504','506','552','562') THEN 'GR Reversal'
            WHEN bwart IN ('122','123')                              THEN 'Return to Vendor'
            WHEN bwart IN ('201','221','241','261','281','291','601') THEN 'Goods Issue'
            WHEN bwart IN ('202','222','242','262','282','292','602') THEN 'GI Reversal'
            WHEN bwart IN ('301','303','305','311','313','315')       THEN 'Transfer Posting'
            WHEN bwart IN ('302','304','306','312','314','316')       THEN 'Transfer Reversal'
            WHEN bwart IN ('541','542','543','544')                   THEN 'Subcontracting'
            WHEN bwart IN ('701','702','711','712')                   THEN 'Stock Transport'
            ELSE 'Other (' || bwart || ')'
        END                                         AS mvmt_category
    FROM "sap_stg_ecc"."stg_sap_mb51_mseg"
)

/* ──────────────────────────────────────────────────────────────
    MAIN SELECT
   ────────────────────────────────────────────────────────────── */
SELECT

    sg.mblnr                                        AS "Material Document",
    sg.mjahr                                        AS "Mat. Doc. Year",
    sg.zeile                                        AS "Item",

    hd.bldat                                        AS "Document Date",
    hd.budat                                        AS "Posting Date",
    hd.cputm                                        AS "Time of Entry",

    sg.bukrs                                        AS "Company Code",
    sg.werks                                        AS "Plant",
    sg.lgort                                        AS "Storage Location",

    sg.bwart                                        AS "Movement Type",
    mc.mvmt_category                                AS "Movement Category",
    sg.shkzg                                        AS "Dr/Cr Indicator",

    sg.matnr                                        AS "Material",
    COALESCE(
        NULLIF(mk.maktx, ''),
        '— no description —'
    )                                               AS "Material Description",
    sg.charg                                        AS "Batch",

    am.qty_signed                                   AS "Qty in Unit of Entry",
    sg.erfme                                        AS "Unit of Entry",
    am.qty_base_signed                              AS "Qty in Base UoM",
    sg.meins                                        AS "Base UoM",

    am.amount_signed                                AS "Amount in LC",

    sg.ebeln                                        AS "Purchase Order",
    sg.ebelp                                        AS "PO Item",
    sg.aufnr                                        AS "Order Number",
    sg.kostl                                        AS "Cost Center",
    sg.prctr                                        AS "Profit Center",

    sg.sgtxt                                        AS "Item Text",
    hd.bktxt                                        AS "Document Header Text",

    hd.usnam                                        AS "User Name",
    hd.tcode                                        AS "Transaction Code"

FROM      "sap_stg_ecc"."stg_sap_mb51_mseg"  sg

LEFT  JOIN "sap_stg_ecc"."stg_sap_mb51_mkpf"  hd
    ON  hd.mblnr = sg.mblnr
    AND hd.mjahr = sg.mjahr

LEFT  JOIN CTE_AMOUNT            am
    ON  am.mblnr = sg.mblnr
    AND am.mjahr = sg.mjahr
    AND am.zeile = sg.zeile

LEFT  JOIN CTE_MVMT_CLASS        mc
    ON  mc.mblnr = sg.mblnr
    AND mc.mjahr = sg.mjahr
    AND mc.zeile = sg.zeile

LEFT  JOIN "sap_stg_ecc"."stg_sap_mb51_makt"  mk
    ON  mk.matnr = sg.matnr

ORDER BY
    hd.budat    DESC,
    sg.mblnr    ASC,
    sg.zeile    ASC;

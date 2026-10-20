/* ============================================================
   MB52 — Inventory Analytics (Warehouse Stocks of Material)
   PostgreSQL
   ============================================================
   Staging tables used (schema: sap_stg_ecc):
     stg_sap_mb52_mard   — Storage Location Stock (unrestricted)
     stg_sap_mb52_mchb   — Batch-Level Stock
     stg_sap_mb52_mara   — Material Master (general data)
     stg_sap_mb52_t001w  — Plant Master
     stg_sap_mb52_t001k  — Valuation Area → Company Code mapping
     stg_sap_mb52_t001   — Company Code Master (name, currency)
     stg_sap_mb52_mbew   — Material Valuation (value, std price)
     stg_sap_mb52_mseg   — GR Line Items (BWART = '101')
     stg_sap_mb52_mkpf   — GR Document Headers (posted on INVENTORY_DATE)

   Design notes:
     • MARD is the anchor table (one row per material + plant + sloc).
     • MCHB batch stock is aggregated per MATNR+WERKS+LGORT so it
       joins 1:1 back to MARD without row multiplication.
     • MBEW is joined on MATNR + BWKEY (valuation area); BWKEY comes
       from T001W, resolved through T001K.
     • MSEG+MKPF represent goods receipts (BWART='101') posted on the
       inventory date only — aligned with MB52 snapshot intent.
     • SHKZG logic follows MB51 convention:
         'S' (Soll/Debit)  → positive quantity / amount
         'H' (Haben/Credit)→ negative quantity / amount

   INVENTORY_DATE filter:
     Replace '20260516' with your target date in YYYYMMDD format,
     or parameterise via your reporting tool.
   ============================================================ */


/* ──────────────────────────────────────────────────────────────
   STEP 1  —  CTE: Batch stock aggregated to MARD grain
   MCHB has one row per (MATNR, WERKS, LGORT, CHARG, SGT_SCAT).
   We collapse to (MATNR, WERKS, LGORT) so the join to MARD is 1:1.
   The batch code and stock segment of the *first* batch are surfaced
   as representative values; total batch-level unrestricted stock
   (CLABS) is summed.
   ────────────────────────────────────────────────────────────── */
WITH CTE_BATCH AS (
    SELECT
        matnr,
        werks,
        lgort,
        /* Representative batch / segment (lowest CHARG per sloc) */
        MIN(charg)                                  AS charg,
        MIN(sgt_scat)                               AS sgt_scat,
        /* Aggregated batch-level unrestricted stock */
        SUM(
            CAST(NULLIF(clabs, '') AS NUMERIC(18,3))
        )                                           AS clabs_total,
        COUNT(DISTINCT charg)                       AS batch_count
    FROM "sap_stg_ecc"."stg_sap_mb52_mchb"
    GROUP BY matnr, werks, lgort
),

/* ──────────────────────────────────────────────────────────────
   STEP 2  —  CTE: Latest GR per material + plant + sloc
   MSEG (BWART='101') is joined to MKPF on the inventory date.
   We take the most recent posting per (MATNR, WERKS, LGORT):
   the GR document date and the sign-corrected quantity received.
   ────────────────────────────────────────────────────────────── */
CTE_GR AS (
    SELECT
        sg.matnr,
        sg.werks,
        sg.lgort,
        /* Latest GR posting date on the inventory date */
        MAX(hd.budat)                               AS gr_budat,
        /* Total GR quantity received on the inventory date (sign-corrected) */
        SUM(
            CASE sg.shkzg
                WHEN 'H' THEN CAST(NULLIF(sg.menge, '') AS NUMERIC(18,3)) * -1
                ELSE           CAST(NULLIF(sg.menge, '') AS NUMERIC(18,3))
            END
        )                                           AS gr_menge_signed,
        /* Total GR value in local currency (sign-corrected) */
        SUM(
            CASE sg.shkzg
                WHEN 'H' THEN CAST(NULLIF(sg.dmbtr, '') AS NUMERIC(18,2)) * -1
                ELSE           CAST(NULLIF(sg.dmbtr, '') AS NUMERIC(18,2))
            END
        )                                           AS gr_dmbtr_signed,
        COUNT(DISTINCT sg.mblnr)                    AS gr_doc_count
    FROM "sap_stg_ecc"."stg_sap_mb52_mseg"  sg
    /* Join to header to apply the INVENTORY_DATE filter on BUDAT */
    INNER JOIN "sap_stg_ecc"."stg_sap_mb52_mkpf" hd
        ON  hd.mblnr = sg.mblnr
        AND hd.mjahr = sg.mjahr
        AND hd.budat = '20260516'           -- ← replace with your INVENTORY_DATE
    WHERE sg.bwart = '101'                  -- Goods Receipt only
    GROUP BY sg.matnr, sg.werks, sg.lgort
),

/* ──────────────────────────────────────────────────────────────
   STEP 3  —  CTE: Valuation area resolution
   T001W.BWKEY → T001K (valuation area to company code)
   → T001 (company master: name, currency).
   Pre-joining these three small config tables avoids a
   multi-join fan-out in the main query.
   ────────────────────────────────────────────────────────────── */
CTE_COMPANY AS (
    SELECT
        pw.werks,
        pw.bwkey,
        pk.bukrs,
        co.butxt                                    AS company_name,
        co.waers                                    AS currency
    FROM      "sap_stg_ecc"."stg_sap_mb52_t001w"  pw
    LEFT JOIN "sap_stg_ecc"."stg_sap_mb52_t001k"  pk
        ON  pk.bwkey = pw.bwkey
    LEFT JOIN "sap_stg_ecc"."stg_sap_mb52_t001"   co
        ON  co.bukrs = pk.bukrs
),

/* ──────────────────────────────────────────────────────────────
   STEP 4  —  CTE: Material valuation
   MBEW is keyed on (MATNR, BWKEY).  Join via BWKEY from CTE_COMPANY.
   We pick the standard price (STPRS), inventory value (SALK3),
   and price control indicator (VPRSV).
   ────────────────────────────────────────────────────────────── */
CTE_VALUATION AS (
    SELECT
        mv.matnr,
        mv.bwkey,
        mv.vprsv                                    AS price_control,
        mv.bwtar                                    AS valuation_type,
        CAST(NULLIF(mv.stprs, '') AS NUMERIC(18,4)) AS stprs,
        CAST(NULLIF(mv.verpr, '') AS NUMERIC(18,4)) AS verpr,
        CAST(NULLIF(mv.peinh, '') AS NUMERIC(18,3)) AS peinh,
        CAST(NULLIF(mv.salk3, '') AS NUMERIC(18,2)) AS salk3,
        CAST(NULLIF(mv.lbkum, '') AS NUMERIC(18,3)) AS lbkum
    FROM "sap_stg_ecc"."stg_sap_mb52_mbew" mv
)

/* ──────────────────────────────────────────────────────────────
   MAIN SELECT — MB52 Inventory Analytics Columns
   Grain: one row per Material + Plant + Storage Location
   ────────────────────────────────────────────────────────────── */
SELECT

    /* ── Snapshot Metadata ──────────────────────────────────── */
    md.inventory_dt                                 AS "Inventory Date",

    /* ── Company & Organisational Data ─────────────────────── */
    cp.bukrs                                        AS "Company Code",
    cp.company_name                                 AS "Company Name",
    cp.currency                                     AS "Currency",
    cp.bwkey                                        AS "Valuation Area",

    /* ── Plant & Storage Location ───────────────────────────── */
    md.werks                                        AS "Plant",
    pw.name1                                        AS "Plant Name",
    pw.land1                                        AS "Country",
    pw.regio                                        AS "Region",
    md.lgort                                        AS "Storage Location",

    /* ── Material Identification ────────────────────────────── */
    md.matnr                                        AS "Material",
    ma.mtart                                        AS "Material Type",
    ma.matkl                                        AS "Material Group",
    ma.mbrsh                                        AS "Industry Sector",
    ma.meins                                        AS "Base Unit of Measure",

    /* ── Stock Quantities (MARD — unrestricted grain) ───────── */
    CAST(NULLIF(md.labst,  '') AS NUMERIC(18,3))    AS "Unrestricted Stock",
    CAST(NULLIF(md.einme,  '') AS NUMERIC(18,3))    AS "Stock in Quality Insp.",
    CAST(NULLIF(md.insme,  '') AS NUMERIC(18,3))    AS "Restricted-Use Stock",
    CAST(NULLIF(md.retme,  '') AS NUMERIC(18,3))    AS "Blocked Stock (Returns)",
    CAST(NULLIF(md.vmlab,  '') AS NUMERIC(18,3))    AS "Prev. Period Unrestr. Stock",
    CAST(NULLIF(md.umlme,  '') AS NUMERIC(18,3))    AS "Stock in Transfer",
    md.lfgja                                        AS "Fiscal Year (Last GR)",
    md.lfmon                                        AS "Fiscal Month (Last GR)",

    /* ── Batch Stock (aggregated from MCHB) ─────────────────── */
    bt.charg                                        AS "Batch (Representative)",
    bt.sgt_scat                                     AS "Stock Segment",
    bt.clabs_total                                  AS "Batch Unrestricted Stock",
    bt.batch_count                                  AS "Batch Count",

    /* ── Valuation (MBEW) ────────────────────────────────────── */
    vl.price_control                                AS "Price Control (V/S)",
    vl.valuation_type                               AS "Valuation Type",
    vl.stprs                                        AS "Standard Price (Per Piece)",
    vl.verpr                                        AS "Moving Avg. Price",
    vl.peinh                                        AS "Price Unit",
    vl.salk3                                        AS "Inventory Value (Total)",
    vl.lbkum                                        AS "Total Valuated Stock",

    /* ── Calculated Value Fields ─────────────────────────────── */
    /* Per-piece value: use STPRS for S-price, VERPR for V-price */
    CASE vl.price_control
        WHEN 'S' THEN vl.stprs
        WHEN 'V' THEN vl.verpr
        ELSE          COALESCE(vl.stprs, vl.verpr)
    END                                             AS "Effective Per Piece Value",

    /* Stock value at effective price × unrestricted qty */
    ROUND(
        CAST(NULLIF(md.labst, '') AS NUMERIC(18,3))
        *
        CASE vl.price_control
            WHEN 'S' THEN vl.stprs
            WHEN 'V' THEN vl.verpr
            ELSE          COALESCE(vl.stprs, vl.verpr)
        END
    , 2)                                            AS "Calculated Stock Value",

    /* ── Goods Receipt on Inventory Date (MSEG+MKPF) ────────── */
    gr.gr_budat                                     AS "Purchase Date (GR)",
    gr.gr_menge_signed                              AS "Purchase Quantity (GR)",
    gr.gr_dmbtr_signed                              AS "GR Amount (LC)",
    gr.gr_doc_count                                 AS "GR Document Count",

    /* ── Material Physical Attributes (MARA) ─────────────────── */
    CAST(NULLIF(ma.ntgew, '') AS NUMERIC(18,3))     AS "Net Weight",
    CAST(NULLIF(ma.brgew, '') AS NUMERIC(18,3))     AS "Gross Weight",
    ma.gewei                                        AS "Weight Unit",
    ma.mfrpn                                        AS "Manufacturer Part No."

FROM      "sap_stg_ecc"."stg_sap_mb52_mard"   md

/* ── Plant master ───────────────────────────────────────────── */
LEFT JOIN "sap_stg_ecc"."stg_sap_mb52_t001w"  pw
    ON  pw.werks = md.werks

/* ── Company / currency (via valuation area) ─────────────────  */
LEFT JOIN CTE_COMPANY                           cp
    ON  cp.werks = md.werks

/* ── Material master ─────────────────────────────────────────── */
LEFT JOIN "sap_stg_ecc"."stg_sap_mb52_mara"   ma
    ON  ma.matnr = md.matnr

/* ── Batch stock (aggregated) ───────────────────────────────── */
LEFT JOIN CTE_BATCH                             bt
    ON  bt.matnr = md.matnr
    AND bt.werks  = md.werks
    AND bt.lgort  = md.lgort

/* ── Valuation (MBEW keyed on MATNR + BWKEY) ───────────────── */
LEFT JOIN CTE_VALUATION                         vl
    ON  vl.matnr = md.matnr
    AND vl.bwkey = cp.bwkey

/* ── Goods receipt on inventory date ────────────────────────── */
LEFT JOIN CTE_GR                                gr
    ON  gr.matnr = md.matnr
    AND gr.werks  = md.werks
    AND gr.lgort  = md.lgort

/* ── Filter: only rows with stock or GR activity ────────────── */
WHERE (
    CAST(NULLIF(md.labst, '') AS NUMERIC(18,3)) > 0
    OR gr.gr_menge_signed IS NOT NULL
)

ORDER BY
    md.werks    ASC,
    md.lgort    ASC,
    md.matnr    ASC;


/* ============================================================
   SUMMARY VIEWS
   Run these separately for aggregated analysis.
   ============================================================ */

/* -- S1: Stock value summary by plant and material type ------- */
/*
SELECT
    md.werks                                        AS "Plant",
    pw.name1                                        AS "Plant Name",
    ma.mtart                                        AS "Material Type",
    ma.matkl                                        AS "Material Group",
    cp.currency                                     AS "Currency",
    COUNT(DISTINCT md.matnr)                        AS "Material Count",
    SUM(CAST(NULLIF(md.labst,'') AS NUMERIC(18,3))) AS "Total Unrestr. Stock",
    SUM(vl.salk3)                                   AS "Total Inventory Value"
FROM "sap_stg_ecc"."stg_sap_mb52_mard"   md
LEFT JOIN "sap_stg_ecc"."stg_sap_mb52_t001w"  pw ON pw.werks = md.werks
LEFT JOIN "sap_stg_ecc"."stg_sap_mb52_mara"   ma ON ma.matnr = md.matnr
LEFT JOIN CTE_COMPANY                           cp ON cp.werks  = md.werks
LEFT JOIN CTE_VALUATION                         vl ON vl.matnr  = md.matnr AND vl.bwkey = cp.bwkey
GROUP BY md.werks, pw.name1, ma.mtart, ma.matkl, cp.currency
ORDER BY SUM(vl.salk3) DESC NULLS LAST;
*/

/* -- S2: Batch stock summary per material + plant ------------- */
/*
SELECT
    mchb.matnr                                      AS "Material",
    mchb.werks                                      AS "Plant",
    mchb.lgort                                      AS "Storage Location",
    COUNT(DISTINCT mchb.charg)                      AS "Batch Count",
    SUM(CAST(NULLIF(mchb.clabs,'') AS NUMERIC(18,3))) AS "Total Batch Unr. Stock",
    MIN(mchb.charg)                                 AS "Earliest Batch",
    MAX(mchb.charg)                                 AS "Latest Batch"
FROM "sap_stg_ecc"."stg_sap_mb52_mchb" mchb
GROUP BY mchb.matnr, mchb.werks, mchb.lgort
ORDER BY mchb.werks, mchb.matnr;
*/

/* -- S3: GR receipts on inventory date by material + plant ---- */
/*
SELECT
    sg.matnr                                        AS "Material",
    sg.werks                                        AS "Plant",
    sg.lgort                                        AS "Storage Location",
    hd.budat                                        AS "GR Posting Date",
    sg.ebeln                                        AS "Purchase Order",
    COUNT(DISTINCT sg.mblnr)                        AS "GR Documents",
    SUM(
        CASE sg.shkzg
            WHEN 'H' THEN CAST(NULLIF(sg.menge,'') AS NUMERIC(18,3)) * -1
            ELSE           CAST(NULLIF(sg.menge,'') AS NUMERIC(18,3))
        END
    )                                               AS "Total GR Qty",
    SUM(
        CASE sg.shkzg
            WHEN 'H' THEN CAST(NULLIF(sg.dmbtr,'') AS NUMERIC(18,2)) * -1
            ELSE           CAST(NULLIF(sg.dmbtr,'') AS NUMERIC(18,2))
        END
    )                                               AS "Total GR Amount (LC)"
FROM "sap_stg_ecc"."stg_sap_mb52_mseg"   sg
INNER JOIN "sap_stg_ecc"."stg_sap_mb52_mkpf" hd
    ON  hd.mblnr = sg.mblnr
    AND hd.mjahr = sg.mjahr
    AND hd.budat = '20260516'           -- ← replace with your INVENTORY_DATE
WHERE sg.bwart = '101'
GROUP BY sg.matnr, sg.werks, sg.lgort, hd.budat, sg.ebeln
ORDER BY hd.budat DESC, sg.werks, sg.matnr;
*/

/* -- S4: Zero-stock materials (in master but no MARD stock) --- */
/*
SELECT
    ma.matnr                                        AS "Material",
    ma.mtart                                        AS "Material Type",
    ma.matkl                                        AS "Material Group",
    ma.meins                                        AS "Base UoM",
    md.werks                                        AS "Plant",
    md.lgort                                        AS "Storage Location",
    CAST(NULLIF(md.labst,'') AS NUMERIC(18,3))      AS "Unrestricted Stock"
FROM "sap_stg_ecc"."stg_sap_mb52_mard"  md
LEFT JOIN "sap_stg_ecc"."stg_sap_mb52_mara" ma ON ma.matnr = md.matnr
WHERE COALESCE(CAST(NULLIF(md.labst,'') AS NUMERIC(18,3)), 0) = 0
ORDER BY md.werks, md.matnr;
*/
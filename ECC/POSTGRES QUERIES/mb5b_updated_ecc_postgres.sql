/* ============================================================
    MB5B — Stock on Posting Date (Updated / Backward Logic) | PostgreSQL
   ============================================================
   Uses a CTE_PARAMS block instead of DECLARE variables.
   Replaces TRY_CAST + ISNULL with CAST(NULLIF(...,'')) + COALESCE.
   Replaces RIGHT/LEN/PATINDEX zero-strip with LTRIM(x,'0').
   Replaces FORMAT(CONVERT(date,x,112),'dd.MM.yyyy') with
     TO_CHAR(TO_DATE(x,'YYYYMMDD'),'DD.MM.YYYY').
   ============================================================ */

WITH CTE_PARAMS AS (
    SELECT
        '20251231' AS date_from,
        '20251231' AS date_to
),

/* ─────────────────────────────────────────────────────────────
   STEP 1: CURRENT STOCK  (MARD → numeric-safe)
   ─────────────────────────────────────────────────────────────*/
STOCK AS (
    SELECT
        matnr,
        werks,
        SUM(
            COALESCE(CAST(REPLACE(labst, ',', '') AS NUMERIC(18,3)), 0) +
            COALESCE(CAST(REPLACE(insme, ',', '') AS NUMERIC(18,3)), 0) +
            COALESCE(CAST(REPLACE(speme, ',', '') AS NUMERIC(18,3)), 0)
        ) AS curr_qty

    FROM "sap_stg_ecc"."stg_sap_mb5b_mard"
    GROUP BY matnr, werks
),

/* ─────────────────────────────────────────────────────────────
   STEP 2: MOVEMENTS  (full cleaning — joins MSEG + MKPF)
   ─────────────────────────────────────────────────────────────*/
MOVEMENTS AS (
    SELECT
        mseg.matnr,
        mseg.werks,
        mseg.shkzg,
        mseg.bukrs,

        /* Safe qty */
        COALESCE(
            CAST(REPLACE(mseg.menge, ',', '') AS NUMERIC(18,3)),
            0
        ) AS menge,

        /* Safe value */
        COALESCE(
            CAST(REPLACE(mseg.dmbtr, ',', '') AS NUMERIC(18,2)),
            0
        ) AS dmbtr,

        mkpf.budat

    FROM "sap_stg_ecc"."stg_sap_mb5b_mseg" mseg
    INNER JOIN "sap_stg_ecc"."stg_sap_mb5b_mkpf" mkpf
        ON mkpf.mblnr = mseg.mblnr
       AND mkpf.mjahr = mseg.mjahr
),

/* ─────────────────────────────────────────────────────────────
   STEP 3: FUTURE MOVEMENTS  (backward logic)
   Movements that happened AFTER date_to — subtract from current
   stock to derive the stock position as of date_to.
   ─────────────────────────────────────────────────────────────*/
FUTURE AS (
    SELECT
        m.matnr,
        m.werks,
        SUM(
            CASE
                WHEN m.budat > p.date_to
                THEN CASE WHEN m.shkzg = 'H' THEN -m.menge ELSE m.menge END
                ELSE 0
            END
        ) AS future_qty,
        SUM(
            CASE
                WHEN m.budat > p.date_to
                THEN CASE WHEN m.shkzg = 'H' THEN -m.dmbtr ELSE m.dmbtr END
                ELSE 0
            END
        ) AS future_val

    FROM MOVEMENTS m
    CROSS JOIN CTE_PARAMS p
    GROUP BY m.matnr, m.werks
),

/* ─────────────────────────────────────────────────────────────
   STEP 4: SAME-DAY MOVEMENTS  (critical fix)
   Receipts and issues posted on date_from are needed to compute
   the opening stock correctly (opening = stock before that day).
   ─────────────────────────────────────────────────────────────*/
DAY_MOV AS (
    SELECT
        m.matnr,
        m.werks,

        SUM(
            CASE WHEN m.budat = p.date_from AND m.shkzg = 'S'
                 THEN m.menge ELSE 0
            END
        ) AS receipt,

        SUM(
            CASE WHEN m.budat = p.date_from AND m.shkzg = 'H'
                 THEN -m.menge ELSE 0
            END
        ) AS issue,

        SUM(
            CASE WHEN m.budat = p.date_from AND m.shkzg = 'S'
                 THEN m.dmbtr ELSE 0
            END
        ) AS receipt_val,

        SUM(
            CASE WHEN m.budat = p.date_from AND m.shkzg = 'H'
                 THEN -m.dmbtr ELSE 0
            END
        ) AS issue_val,

        MAX(m.bukrs) AS bukrs

    FROM MOVEMENTS m
    CROSS JOIN CTE_PARAMS p
    GROUP BY m.matnr, m.werks
)

/* ─────────────────────────────────────────────────────────────
   FINAL OUTPUT
   ─────────────────────────────────────────────────────────────*/
SELECT

    s.werks                                             AS "ValA",
    t001.butxt                                          AS "Company Name",

    /* Remove leading zeros from material number */
    LTRIM(s.matnr, '0')                                 AS "Material",
    makt.maktx                                          AS "Material Description",

    TO_CHAR(TO_DATE(p.date_from, 'YYYYMMDD'), 'DD.MM.YYYY') AS "From Date",
    TO_CHAR(TO_DATE(p.date_to,   'YYYYMMDD'), 'DD.MM.YYYY') AS "To Date",

    /* ─────────────────────────────────────────────────────────
       STOCK  (backward correct)
       Opening = current stock − future movements − today's movements
    ──────────────────────────────────────────────────────────── */
    CAST(
        s.curr_qty
        - COALESCE(f.future_qty, 0)
        - COALESCE(d.receipt, 0)
        - COALESCE(d.issue,   0)
    AS NUMERIC(18,3))                                   AS "Opening Stock",

    COALESCE(d.receipt, 0)                              AS "Total Receipt Qties",
    COALESCE(d.issue,   0)                              AS "Total Issue Quantities",

    CAST(
        s.curr_qty - COALESCE(f.future_qty, 0)
    AS NUMERIC(18,3))                                   AS "Closing Stock",

    /* ─────────────────────────────────────────────────────────
       UNIT  (base unit — matches DB currently)
    ──────────────────────────────────────────────────────────── */
    'KAR'                                               AS "BUn",   -- Replace with meins if available

    /* ─────────────────────────────────────────────────────────
       VALUE
    ──────────────────────────────────────────────────────────── */
    CAST(
        COALESCE(d.receipt_val, 0)
        + COALESCE(d.issue_val, 0)
    AS NUMERIC(18,2))                                   AS "Total Movement Value",

    CAST(
        COALESCE(f.future_val, 0) * -1
    AS NUMERIC(18,2))                                   AS "Closing Value",

    t001.waers                                          AS "Crcy"

FROM STOCK s

CROSS JOIN CTE_PARAMS p

LEFT JOIN FUTURE f
    ON  f.matnr = s.matnr
    AND f.werks  = s.werks

LEFT JOIN DAY_MOV d
    ON  d.matnr = s.matnr
    AND d.werks  = s.werks

LEFT JOIN "sap_stg_ecc"."stg_sap_mb5b_t001" t001
    ON  t001.bukrs = d.bukrs

LEFT JOIN "sap_stg_ecc"."stg_sap_mb5b_makt" makt
    ON  makt.matnr = s.matnr
    AND makt.spras = 'E'

WHERE
    s.matnr = '000000000011000001'
    AND s.werks = '2601';

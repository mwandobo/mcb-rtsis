WITH params AS (
    SELECT
        CURRENT_DATE AS report_date,
        CURRENT_TIMESTAMP AS report_timestamp
    FROM SYSIBM.SYSDUMMY1
),

clean_gte AS (
    SELECT
        gte.*,

        CASE
            WHEN gte.DC_AMOUNT IS NULL
                THEN NULL

            WHEN TRIM(gte.DC_AMOUNT) = ''
                THEN NULL

            WHEN
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                    TRIM(gte.DC_AMOUNT),
                    '0', ''),
                    '1', ''),
                    '2', ''),
                    '3', ''),
                    '4', ''),
                    '5', ''),
                    '6', ''),
                    '7', ''),
                    '8', ''),
                    '9', ''),
                    ',', ''),
                    '.', ''),
                    '-', ''),
                    '+', '')
                = ''

            THEN DECIMAL(
                REPLACE(TRIM(gte.DC_AMOUNT), ',', ''),
                18,
                2
            )

            ELSE NULL
        END AS numeric_dc_amount

    FROM GLI_TRX_EXTRACT gte
),

latest_fx_raw AS (
    SELECT
        fr.fk_currencyid_curr,
        fr.rate
    FROM fixing_rate fr
    WHERE (
        fr.fk_currencyid_curr,
        fr.activation_date,
        fr.activation_time
    ) IN (
        SELECT
            fk_currencyid_curr,
            activation_date,
            MAX(activation_time)
        FROM fixing_rate
        WHERE activation_date = (
            SELECT MAX(b.activation_date)
            FROM fixing_rate b
            WHERE b.activation_date <= CURRENT_DATE
        )
        GROUP BY
            fk_currencyid_curr,
            activation_date
    )
),

latest_fx AS (
    SELECT
        fk_currencyid_curr,

        CASE
            WHEN rate IS NULL
                THEN NULL

            WHEN TRIM(CHAR(rate)) = ''
                THEN NULL

            WHEN
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                    TRIM(CHAR(rate)),
                    '0', ''),
                    '1', ''),
                    '2', ''),
                    '3', ''),
                    '4', ''),
                    '5', ''),
                    '6', ''),
                    '7', ''),
                    '8', ''),
                    '9', ''),
                    ',', ''),
                    '.', ''),
                    '-', ''),
                    '+', '')
                = ''

            THEN DECIMAL(
                REPLACE(TRIM(CHAR(rate)), ',', ''),
                18,
                6
            )

            ELSE NULL
        END AS numeric_rate

    FROM latest_fx_raw
)

SELECT

    /* ============================================================
       REPORTING DATE
       ============================================================ */
    VARCHAR_FORMAT(
        p.report_timestamp,
        'DDMMYYYYHH24MI'
    ) AS reportingDate,


    /* ============================================================
       ASSET TYPE
       ============================================================ */
    CASE
        WHEN gl.EXTERNAL_GLACCOUNT = '100017000'
            THEN 'Gold'

        WHEN gl.EXTERNAL_GLACCOUNT = '144000032'
            THEN 'StampAccount'

        WHEN gl.EXTERNAL_GLACCOUNT IN (
            '144000015',
            '144000047',
            '144000048',
            '144000050',
            '144000051',
            '144000054',
            '144000058',
            '144000061',
            '144000062',
            '144000074'
        )
            THEN 'SundryDebtors'

        WHEN gl.EXTERNAL_GLACCOUNT IN (
            '230000007',
            '230000071',
            '145000001',
            '230000079'
        )
            THEN 'Trade Credit and advances'

        WHEN gl.EXTERNAL_GLACCOUNT IN (
            '144000006',
            '144000066'
        )
            THEN 'PrepaidExpenses'

        WHEN gl.EXTERNAL_GLACCOUNT IN (
            '170150001',
            '170150002',
            '171030001',
            '144000052'
        )
            THEN 'otherIntangible asset'

        ELSE 'MiscellaneousAssets'
    END AS assetType,


    /* ============================================================
       TRANSACTION DATE
       ============================================================ */
    CASE
        WHEN gte.TRN_DATE IS NOT NULL
        THEN VARCHAR_FORMAT(
            gte.TRN_DATE,
            'DDMMYYYYHH24MI'
        )
        ELSE NULL
    END AS transactionDate,


    /* ============================================================
       MATURITY DATE
       ============================================================ */
    CASE
        WHEN gte.AVAILABILITY_DATE IS NOT NULL
        THEN VARCHAR_FORMAT(
            gte.AVAILABILITY_DATE,
            'DDMMYYYYHH24MI'
        )
        ELSE NULL
    END AS maturityDate,


    /* ============================================================
       DEBTOR NAME
       ============================================================ */
    CASE
        WHEN TRIM(
            COALESCE(RTRIM(WC.FIRST_NAME), '')
            || ' ' ||
            COALESCE(RTRIM(WC.MIDDLE_NAME), '')
            || ' ' ||
            COALESCE(RTRIM(WC.SURNAME), '')
        ) <> ''
            THEN TRIM(
                COALESCE(RTRIM(WC.FIRST_NAME), '')
                || ' ' ||
                COALESCE(RTRIM(WC.MIDDLE_NAME), '')
                || ' ' ||
                COALESCE(RTRIM(WC.SURNAME), '')
            )
        ELSE 'Unknown'
    END AS debtorName,


    /* ============================================================
       DEBTOR COUNTRY
       ============================================================ */
    CASE
        WHEN TRIM(
            UPPER(
                COALESCE(WC.NATIONAL_DESCRIPTION, '')
            )
        ) LIKE '%TANZANIAN%'
            THEN 'Tanzania'

        ELSE 'Unknown'
    END AS debtorCountry,


    /* ============================================================
       CURRENCY
       ============================================================ */
    TRIM(gte.CURRENCY_SHORT_DES) AS currency,


    /* ============================================================
       ORIGINAL AMOUNT
       ============================================================ */
    gte.numeric_dc_amount AS orgAmount,


    /* ============================================================
       USD AMOUNT
       ============================================================ */
    CASE
        WHEN gte.numeric_dc_amount IS NULL
            THEN NULL

        WHEN gte.CURRENCY_SHORT_DES = 'TZS'
            THEN DECIMAL(0, 18, 2)

        WHEN gte.CURRENCY_SHORT_DES = 'USD'
            THEN DECIMAL(
                gte.numeric_dc_amount,
                18,
                2
            )

        WHEN fx.numeric_rate IS NULL
            THEN NULL

        WHEN fx.numeric_rate = 0
            THEN NULL

        ELSE
            DECIMAL(
                DECIMAL(
                    gte.numeric_dc_amount,
                    18,
                    2
                )
                /
                DECIMAL(
                    fx.numeric_rate,
                    18,
                    6
                ),
                18,
                2
            )
    END AS usdAmount,


    /* ============================================================
       TZS AMOUNT
       ============================================================ */
    CASE
        WHEN gte.numeric_dc_amount IS NULL
            THEN NULL

        WHEN gte.CURRENCY_SHORT_DES = 'TZS'
            THEN DECIMAL(
                gte.numeric_dc_amount,
                18,
                2
            )

        WHEN gte.CURRENCY_SHORT_DES = 'USD'
             AND fx.numeric_rate IS NOT NULL
            THEN DECIMAL(
                DECIMAL(
                    gte.numeric_dc_amount,
                    18,
                    2
                )
                *
                DECIMAL(
                    fx.numeric_rate,
                    18,
                    6
                ),
                18,
                2
            )

        ELSE
            DECIMAL(
                gte.numeric_dc_amount,
                18,
                2
            )
    END AS tzsAmount,


    /* ============================================================
       SECTOR SNA CLASSIFICATION
       ============================================================ */
    CASE
        WHEN gl.EXTERNAL_GLACCOUNT IN (
            '144000001',
            '144000039',
            '144000043',
            '101010001',
            '101240001'
        )
            THEN 'Other Depository Corporations'

        WHEN gl.EXTERNAL_GLACCOUNT IN (
            '144000047',
            '144000048',
            '144000050',
            '144000051',
            '144000058',
            '144000061',
            '144000062',
            '144000074'
        )
            THEN 'Other Financial Intermediary'

        WHEN gl.EXTERNAL_GLACCOUNT IN (
            '230000007',
            '145000001',
            '230000071'
        )
            THEN 'Households'

        WHEN gl.EXTERNAL_GLACCOUNT = '112020005'
            THEN 'Insurance Companies'

        WHEN gl.EXTERNAL_GLACCOUNT = '143000001'
            THEN 'Central Government'

        WHEN gl.EXTERNAL_GLACCOUNT = '100017000'
            THEN 'Other Non-Financial Corporations'

        WHEN COALESCE(WC.CUST_ID, '') <> ''
             AND TRIM(
                 UPPER(
                     COALESCE(
                         WC.NATIONAL_DESCRIPTION,
                         ''
                     )
                 )
             ) LIKE '%TANZANIAN%'
            THEN 'Households'

        WHEN COALESCE(WC.CUST_ID, '') <> ''
            THEN 'Other Non-Financial Corporations'

        ELSE 'Other Non-Financial Corporations'
    END AS sectorSnaClassification,


    /* ============================================================
       PAST DUE DAYS
       ============================================================ */
    CASE
        WHEN gte.AVAILABILITY_DATE IS NULL
            THEN NULL

        WHEN DATE(gte.AVAILABILITY_DATE) < p.report_date
            THEN
                DAYS(p.report_date)
                - DAYS(DATE(gte.AVAILABILITY_DATE))

        ELSE 0
    END AS pastDueDays,


    /* ============================================================
       ASSET CLASSIFICATION
       ============================================================ */
    CASE
        WHEN gte.AVAILABILITY_DATE IS NULL
            THEN 1

        WHEN DATE(gte.AVAILABILITY_DATE) >= p.report_date
            THEN 1

        WHEN
            DAYS(p.report_date)
            - DAYS(DATE(gte.AVAILABILITY_DATE))
            <= 30
            THEN 1

        WHEN
            DAYS(p.report_date)
            - DAYS(DATE(gte.AVAILABILITY_DATE))
            <= 90
            THEN 2

        ELSE 3
    END AS assetClassificationCategory,


    /* ============================================================
       PROVISIONS
       ============================================================ */
    DECIMAL(0, 15, 2) AS allowanceProbableLoss,

    DECIMAL(0, 15, 2) AS botProvision


FROM clean_gte gte

CROSS JOIN params p

LEFT JOIN CUSTOMER C
    ON C.CUST_ID = gte.CUST_ID

LEFT JOIN W_DIM_CUSTOMER WC
    ON WC.CUST_ID = gte.CUST_ID

LEFT JOIN GLG_ACCOUNT gl
    ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO

LEFT JOIN CURRENCY curr
    ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES

LEFT JOIN latest_fx fx
    ON fx.fk_currencyid_curr = curr.ID_CURRENCY


WHERE gl.EXTERNAL_GLACCOUNT IN (
    '100017000',
    '101010001',
    '101240001',
    '112020005',
    '143000001',
    '144000001',
    '144000015',
    '144000032',
    '144000039',
    '144000043',
    '144000046',
    '144000047',
    '144000048',
    '144000050',
    '144000051',
    '144000054',
    '144000057',
    '144000058',
    '144000061',
    '144000062',
    '144000066',
    '144000074',
    '145000001',
    '230000007',
    '230000013',
    '230000014',
    '230000071',
    '230000079',
    '144000006',
    '144000052',
    '170150001',
    '170150002',
    '171030001',
    '705190001',
    '705190002',
    '705190003',
    '144000020'
)
  -- Required BoT attributes must come from an authoritative source value.
  AND gte.TRN_DATE IS NOT NULL
  AND gte.AVAILABILITY_DATE IS NOT NULL
  AND TRIM(COALESCE(gte.CURRENCY_SHORT_DES, '')) <> ''
  AND gte.numeric_dc_amount IS NOT NULL;

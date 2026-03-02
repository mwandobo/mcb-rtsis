WITH district_wards AS (SELECT DISTINCT DISTRICT,
                                        WARD,
                                        ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY WARD) AS rn,
                                        COUNT(*) OVER (PARTITION BY DISTRICT)                   AS total_wards
                        FROM bank_location_lookup_v2)
        ,
     region_districts AS (SELECT REGION,
                                 DISTRICT,
                                 ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY DISTRICT) AS rn,
                                 COUNT(*) OVER (PARTITION BY REGION)                       AS total_districts
                          FROM bank_location_lookup_v2
                          GROUP BY REGION, DISTRICT)
SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
       gte.CUST_ID                                       AS clientIdentificationNumber,
       PA.ACCOUNT_NUMBER                                 AS accountNumber,
       wdc.NAME_STANDARD                                 AS accountName,
       'Ordinary'                                        AS customerCategory,
       'TANZANIA, UNITED REPUBLIC OF'                    AS customerCountry,
       pa.MONOTORING_UNIT                                AS branchCode,
       CASE
           WHEN pa.PRFT_SYSTEM = 4 THEN 'Staff'
           WHEN pa.PRFT_SYSTEM = 3 THEN 'Individual'
           END                                           AS clientType,
       'Domestic banks unrelated'                        AS relationshipType,
       COALESCE(
               loc_district_region.DISTRICT,
               loc_district_from_ward.DISTRICT,
               loc_district_from_city.DISTRICT,
               loc_district_from_region.DISTRICT
       )                                                 AS district,
       COALESCE(
               loc_region_city.REGION,
               loc_region_dist.REGION,
               loc_region_from_district.REGION,
               loc_region_from_ward.REGION,
               'Dar es Salaam'
       )                                                 AS region,
       p.DESCRIPTION                                     AS accountProductName,
       CASE
           WHEN pa.PRFT_SYSTEM = 4 THEN 'Current'
           ELSE 'Saving'
           END
                                                         AS accountType,
       CASE
           WHEN pa.PRFT_SYSTEM = 4 THEN 'Normal'
           ELSE NULL
           END                                           AS accountSubType,
       'Deposit from public'                             AS depositCategory,
       CASE
           WHEN pa.ACC_STATUS = 1 THEN 'active'
           WHEN pa.ACC_STATUS = 3 THEN 'closed'
           ELSE 'inactive'
           END                                           AS depositAccountStatus,
       VARCHAR(gte.FK_UNITCODETRXUNIT) || '-' ||
       TRIM(gte.FK_USRCODE) || '-' ||
       VARCHAR(gte.LINE_NUM) || '-' ||
       VARCHAR(gte.TRN_DATE) || '-' ||
       VARCHAR(gte.TRN_SNUM)                             AS transactionUniqueRef,
       VARCHAR_FORMAT(gte.TMSTAMP, 'DDMMYYYYHHMM')       AS timeStamp,
       'Branch'                                          AS serviceChannel,
       gte.CURRENCY_SHORT_DES                            AS currency,
       'Deposit'                                         AS transactionType,
       gte.DC_AMOUNT                                     AS orgTransactionAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT
           WHEN gte.CURRENCY_SHORT_DES <> 'USD' THEN DECIMAL(gte.DC_AMOUNT / fx.RATE, 18, 2)
           ELSE NULL
           END                                           AS usdTransactionAmount,

       -- TZS Amount: convert only if USD, otherwise use as is
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN gte.DC_AMOUNT * fx.RATE
           ELSE
               gte.DC_AMOUNT
           END                                           AS tzsTransactionAmount,
       gte.JUSTIFIC_DESCR                                AS transactionPurposes,
       'Households'                                      AS sectorSnaClassification,
       null                                              AS lienNumber,
       null                                              AS orgAmountLien,
       null                                              AS usdAmountLien,
       null                                              AS tzsAmountLien,
       wdc.CUSTOMER_BEGIN_DAT                            AS contractDate,
       null                                              AS maturityDate,
       0                                                 AS annualInterestRate,
       'norminal'                                        AS interestRateType,
       0                                                 AS orgInterestAmount,
       0                                                 AS usdInterestAmount,

       -- TZS Amount: convert only if USD, otherwise use as is
       0                                                 AS tzsInterestAmount
FROM GLI_TRX_EXTRACT gte

         -- Join Currency Using SHORT_DESCR
         -- =========================================
         LEFT JOIN CURRENCY curr
                   ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES

    -- =========================================
    -- Latest Fixing Rate Per Currency
    -- =========================================
         LEFT JOIN (SELECT fr.fk_currencyid_curr,
                           fr.rate
                    FROM fixing_rate fr
                    WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN
                          (SELECT fk_currencyid_curr,
                                  activation_date,
                                  MAX(activation_time)
                           FROM fixing_rate
                           WHERE activation_date = (SELECT MAX(b.activation_date)
                                                    FROM fixing_rate b
                                                    WHERE b.activation_date <= CURRENT_DATE)
                           GROUP BY fk_currencyid_curr, activation_date)) fx
                   ON fx.fk_currencyid_curr = curr.ID_CURRENCY

         LEFT JOIN (SELECT *
                    FROM (SELECT wdc.*,
                                 ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY CUSTOMER_BEGIN_DAT DESC) rn
                          FROM W_DIM_CUSTOMER wdc)
                    WHERE rn = 1) wdc ON wdc.CUST_ID = gte.CUST_ID

         LEFT JOIN cust_address c_address
                   ON c_address.fk_customercust_id = wdc.cust_id
                       AND c_address.communication_addr = '1'
                       AND c_address.entry_status = '1'

    --lookup
         LEFT JOIN (SELECT REGION,
                           ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY REGION) AS rn
                    FROM bank_location_lookup_v2) loc_region_birth_city
                   ON REPLACE(
                              REPLACE(
                                      REPLACE(
                                              REPLACE(UPPER(TRIM(wdc.BIRTHPLACE)), ' ', ''),
                                              '-', ''),
                                      '_', ''),
                              ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_region_birth_city.REGION)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                          || '%'
                       AND loc_region_birth_city.rn = 1

    -- fallback on district to region
         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY REGION) AS rn
                    FROM bank_location_lookup_v2) loc_birth_region_from_district
                   ON loc_region_birth_city.REGION IS NULL
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(wdc.BIRTHPLACE)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_birth_region_from_district.DISTRICT)),
                                                               ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'
                       AND loc_birth_region_from_district.rn = 1

-- fallback on ward
         LEFT JOIN (SELECT REGION,
                           WARD,
                           ROW_NUMBER() OVER (PARTITION BY WARD ORDER BY REGION) AS rn
                    FROM bank_location_lookup_v2) loc_birth_region_from_ward
                   ON loc_region_birth_city.REGION IS NULL
                       AND loc_birth_region_from_district.REGION IS NULL
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(wdc.BIRTHPLACE)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_birth_region_from_ward.WARD)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'
                       AND loc_birth_region_from_ward.rn = 1
    --current location lookup

    --current location lookup
    --region
    --no fallback
         LEFT JOIN (SELECT REGION,
                           ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY REGION) AS rn
                    FROM bank_location_lookup_v2) loc_region_city
                   ON REPLACE(
                              REPLACE(
                                      REPLACE(
                                              REPLACE(UPPER(TRIM(c_address.CITY)), ' ', ''),
                                              '-', ''),
                                      '_', ''),
                              ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_region_city.REGION)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                          || '%'
                       AND loc_region_city.rn = 1

-- fallback on district to region
         LEFT JOIN (SELECT REGION,
                           ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY REGION) AS rn
                    FROM bank_location_lookup_v2) loc_region_dist
                   ON loc_region_city.REGION IS NULL
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(c_address.REGION)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_region_dist.REGION)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'
                       AND loc_region_dist.rn = 1

    --fallback to district then take the region
         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY REGION) AS rn
                    FROM bank_location_lookup_v2) loc_region_from_district
                   ON loc_region_city.REGION IS NULL
                       AND loc_region_dist.REGION IS NULL
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(c_address.REGION)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_region_from_district.DISTRICT)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'
                       AND loc_region_from_district.rn = 1

    -- fallback to ward then take the region
         LEFT JOIN (SELECT REGION,
                           WARD,
                           ROW_NUMBER() OVER (PARTITION BY WARD ORDER BY REGION) AS rn
                    FROM bank_location_lookup_v2) loc_region_from_ward
                   ON loc_region_city.REGION IS NULL
                       AND loc_region_dist.REGION IS NULL
                       AND loc_region_from_district.REGION IS NULL
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(c_address.ADDRESS_1)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_region_from_ward.WARD)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'
                       AND loc_region_from_ward.rn = 1
    --end of region join and lookups

    --district-mapping
    --no fallback
         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           ROW_NUMBER() OVER (PARTITION BY REGION, DISTRICT ORDER BY DISTRICT) AS rn
                    FROM bank_location_lookup_v2) loc_district_region
                   ON loc_district_region.rn = 1
                       AND COALESCE(
                                   loc_region_city.REGION,
                                   loc_region_dist.REGION,
                                   loc_region_from_district.REGION,
                                   loc_region_from_ward.REGION
                           ) = loc_district_region.REGION
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(c_address.REGION)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_district_region.DISTRICT)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'
    -- ward text → district

         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           WARD,
                           ROW_NUMBER() OVER (PARTITION BY REGION, DISTRICT ORDER BY DISTRICT) AS rn
                    FROM bank_location_lookup_v2) loc_district_from_ward
                   ON loc_district_region.DISTRICT IS NULL
                       AND loc_district_from_ward.rn = 1
                       AND COALESCE(
                                   loc_region_city.REGION,
                                   loc_region_dist.REGION,
                                   loc_region_from_district.REGION,
                                   loc_region_from_ward.REGION
                           ) = loc_district_from_ward.REGION
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(c_address.ADDRESS_1)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_district_from_ward.WARD)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'

         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           ROW_NUMBER() OVER (PARTITION BY REGION, DISTRICT ORDER BY DISTRICT) AS rn
                    FROM bank_location_lookup_v2) loc_district_from_city
                   ON loc_district_region.DISTRICT IS NULL
                       AND loc_district_from_ward.DISTRICT IS NULL
                       AND loc_district_from_city.rn = 1
                       AND COALESCE(
                                   loc_region_city.REGION,
                                   loc_region_dist.REGION,
                                   loc_region_from_district.REGION,
                                   loc_region_from_ward.REGION
                           ) = loc_district_from_city.REGION
                       AND REPLACE(
                                   REPLACE(
                                           REPLACE(
                                                   REPLACE(UPPER(TRIM(c_address.ADDRESS_1)), ' ', ''),
                                                   '-', ''),
                                           '_', ''),
                                   ',', '')
                          LIKE '%' ||
                               REPLACE(
                                       REPLACE(
                                               REPLACE(
                                                       REPLACE(UPPER(TRIM(loc_district_from_city.DISTRICT)), ' ', ''),
                                                       '-', ''),
                                               '_', ''),
                                       ',', '')
                               || '%'
-- fallback to take random district from the ward
         LEFT JOIN (SELECT REGION,
                           DISTRICT,
                           ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY DISTRICT ) AS rn
                    FROM bank_location_lookup_v2) loc_district_from_region
                   ON loc_district_region.DISTRICT IS NULL
                       AND loc_district_from_ward.DISTRICT IS NULL
                       AND loc_district_from_city.DISTRICT IS NULL
                       AND loc_district_from_region.rn = 1
                       AND loc_district_from_region.REGION =
                           COALESCE(
                                   loc_region_city.REGION,
                                   loc_region_dist.REGION,
                                   loc_region_from_district.REGION,
                                   loc_region_from_ward.REGION,
                                   'Dar es Salaam'
                           )

         LEFT JOIN district_wards ward_selection
                   ON ward_selection.DISTRICT = COALESCE(
                           loc_district_region.DISTRICT,
                           loc_district_from_ward.DISTRICT,
                           loc_district_from_city.DISTRICT,
                           loc_district_from_region.DISTRICT,
                           'Dar es Salaam'
                                                )
                       AND
                      ward_selection.rn = MOD(ASCII(SUBSTR(TRIM(wdc.CUST_ID), 1, 1)), ward_selection.total_wards) + 1
    -- end of district mapping


         LEFT JOIN region_districts birth_district_pick
                   ON birth_district_pick.REGION =
                      COALESCE(
                              loc_region_birth_city.REGION,
                              loc_birth_region_from_district.REGION,
                              loc_birth_region_from_ward.REGION,
                              loc_region_city.REGION,
                              loc_region_dist.REGION,
                              loc_region_from_district.REGION,
                              loc_region_from_ward.REGION,
                              'Dar es Salaam'
                      )
                       AND birth_district_pick.rn =
                           MOD(
                                   ASCII(SUBSTR(TRIM(wdc.CUST_ID), 1, 1)),
                                   birth_district_pick.total_districts
                           ) + 1
    -- end of mapping


         LEFT JOIN PRODUCT p ON p.ID_PRODUCT = gte.ID_PRODUCT
         LEFT JOIN (SELECT *
                    FROM (SELECT pa.*,
                                 ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY ACCOUNT_NUMBER) rn
                          FROM PROFITS_ACCOUNT pa
                          WHERE PRFT_SYSTEM = 3)
                    WHERE rn = 1) pa ON pa.CUST_ID = gte.CUST_ID
WHERE gte.ID_PRODUCT IN (31201, 31202, 31220);


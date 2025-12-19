-- Branch Information RTSIS Report
-- Based on RTSIS requirements for branch information reporting
-- Created for MCB Bank Tanzania
-- Date: December 18, 2025

SELECT
    /* =========================
       REPORTING INFORMATION
       ========================= */

    -- Reporting Date and Time (DDMMYYYYHHMM format)
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,

    /* =========================
       BRANCH IDENTIFICATION
       ========================= */

    -- Branch Name
    COALESCE(
            u.UNIT_NAME,
            u.UNIT_NAME_LATIN,
            'Unknown Branch'
    )                                                 AS branchName,
    117039447                                         AS taxIdentificationNumber,
    CASE
        WHEN u.CODE = 201 THEN 'BL20000102884'
        WHEN u.CODE = 200 THEN 'BL20000102884'
        END                                           AS businessLicense,


    -- Branch Code (Sort Code)
    CAST(u.CODE AS VARCHAR(10))                       AS branchCode,

    -- QR FSR Code (Financial Services Reporting Code)
    COALESCE(
            gd_fsr.LATIN_DESC,
            'FSR-' || CAST(u.CODE AS VARCHAR(10))
    )                                                 AS qrFsrCode,

    /* =========================
       LOCATION INFORMATION
       ========================= */

    -- Region
    COALESCE(
            gd_region.LATIN_DESC,
            gd_region.DESCRIPTION,
            'Unknown Region'
    )                                                 AS region,

    -- District
    COALESCE(
            bd.DESCRIPTION,
            gd_district.LATIN_DESC,
            'Unknown District'
    )                                                 AS district,

    -- Ward
    COALESCE(
            gd_ward.LATIN_DESC,
            'Unknown Ward'
    )                                                 AS ward,

    -- Street (Optional)
    COALESCE(
            u.LC_STREET_NAME,
            u.ADDRESS,
            u.ADDRESS_LATIN,
            u.ADDRESS_2
    )                                                 AS street,

    -- House Number (Optional)
    COALESCE(
            u.PLOT_STREET,
            u.BUILDING_UNIT
    )                                                 AS houseNumber,

    -- Postal Code (Optional)
    COALESCE(
            u.ZIP_CODE,
            u.PO_BOX
    )                                                 AS postalCode,

    -- GPS Coordinates
    CASE
        WHEN u.LATITUDE_LOCATION IS NOT NULL AND u.LONGITUDE_LOCATION IS NOT NULL
            THEN TRIM(u.LATITUDE_LOCATION) || ',' || TRIM(u.LONGITUDE_LOCATION)
        WHEN u.GEO_AREA IS NOT NULL
            THEN u.GEO_AREA
        ELSE '0.0000,0.0000' -- Default coordinates - should be updated
        END                                           AS gpsCoordinates,

    /* =========================
       SERVICES INFORMATION
       ========================= */

    -- Banking Services (D104 lookup)
    COALESCE(
            gd_services.LATIN_DESC,
            CASE
                WHEN u.CS_UNIT = '1' THEN 'Full Banking Services'
                ELSE 'Limited Banking Services'
                END
    )                                                 AS bankingServices,

    -- Mobile Money Services (D70 lookup - Optional)
    COALESCE(
            gd_mobile.LATIN_DESC,
            'Not Available'
    )                                                 AS mobileMoneyServices,

    /* =========================
       OPERATIONAL INFORMATION
       ========================= */

    -- Registration Date (Branch Opening Date)
    VARCHAR_FORMAT(
            COALESCE(u.OPEN_DATE, DATE(u.TMSTAMP), CURRENT_DATE),
            'DDMMYYYYHHMM'
    )                                                 AS registrationDate,

    -- Branch Status (D64 lookup)
    CASE
        WHEN u.ENTRY_STATUS = '1' AND u.INACTIVE_UNIT = '0' THEN 'Active'
        WHEN u.ENTRY_STATUS = '1' AND u.INACTIVE_UNIT = '1' THEN 'Inactive'
        WHEN u.ENTRY_STATUS = '0' THEN 'Closed'
        ELSE 'Unknown'
        END                                           AS branchStatus,

    -- Closure Date (Optional - for closed branches)
    CASE
        WHEN u.ENTRY_STATUS = '0' OR u.INACTIVE_UNIT = '1'
            THEN VARCHAR_FORMAT(CURRENT_DATE, 'DDMMYYYYHHMM') -- Placeholder
        ELSE NULL
        END                                           AS closureDate,

    /* =========================
       CONTACT INFORMATION
       ========================= */

    -- Contact Person (Branch Manager)
    COALESCE(
            emp.FIRST_NAME || ' ' || emp.LAST_NAME,
            'Branch Manager'
    )                                                 AS contactPerson,

    -- Telephone Number
    COALESCE(
            u.TELEPHONE_1,
            u.TELEPHONE_2,
            '255000000000' -- Default - should be updated
    )                                                 AS telephoneNumber,

    -- Alternate Telephone Number (Optional)
    CASE
        WHEN u.TELEPHONE_1 IS NOT NULL AND u.TELEPHONE_2 IS NOT NULL
            AND u.TELEPHONE_1 != u.TELEPHONE_2
            THEN u.TELEPHONE_2
        ELSE u.FAX
        END                                           AS altTelephoneNumber,

    /* =========================
       BRANCH CLASSIFICATION
       ========================= */

    -- Branch Category (D106 lookup)
    COALESCE(
            gd_category.LATIN_DESC,
            CASE
                WHEN u.CS_HEAD_UNIT IS NULL THEN 'Head Office'
                WHEN u.CS_UNIT = '1' THEN 'Full Service Branch'
                ELSE 'Sub Branch'
                END
    )                                                 AS branchCategory,

    /* =========================
       ADDITIONAL INFORMATION FOR TRACKING
       ========================= */

    -- Unit Code (for reference)
    u.CODE                                            AS unitCode,

    -- Head Unit (Parent Branch)
    u.CS_HEAD_UNIT                                    AS headUnit,

    -- City
    COALESCE(u.CITY, u.CITY_LATIN)                    AS city,

    -- Email
    u.EMAIL                                           AS email,

    -- Country Flag
    u.COUNTRY_FLAG                                    AS countryFlag

FROM UNIT u

         /* ===== BANK PARAMETERS (FOR HEAD OFFICE INFO) ===== */
         LEFT JOIN BANK_PARAMETERS bp
                   ON 1 = 1 -- Single row table

    /* ===== DISTRICT INFORMATION ===== */
         LEFT JOIN BDG_DISTRICT bd
                   ON bd.ID = u.FK_BDG_DISTRICTID

    /* ===== BRANCH MANAGER INFORMATION ===== */
         LEFT JOIN BANKEMPLOYEE emp
                   ON emp.ID = (SELECT MIN(be.ID)
                                FROM BANKEMPLOYEE be
                                WHERE be.EMPL_STATUS = '1'
                       -- Add logic to find branch manager if available
                   )

    /* ===== REGION LOOKUP ===== */
         LEFT JOIN GENERIC_DETAIL gd_region
                   ON gd_region.FK_GENERIC_HEADPAR = u.FKGH_RESIDES_IN_RE
                       AND gd_region.SERIAL_NUM = u.FKGD_RESIDES_IN_RE
                       AND gd_region.ENTRY_STATUS = '1'

    /* ===== DISTRICT LOOKUP ===== */
         LEFT JOIN GENERIC_DETAIL gd_district
                   ON gd_district.FK_GENERIC_HEADPAR = u.FKGH_RESIDES_IN_R1
                       AND gd_district.SERIAL_NUM = u.FKGD_RESIDES_IN_R1
                       AND gd_district.ENTRY_STATUS = '1'

    /* ===== WARD LOOKUP ===== */
         LEFT JOIN GENERIC_DETAIL gd_ward
                   ON gd_ward.FK_GENERIC_HEADPAR = u.FKGH_RESID_REGION3
                       AND gd_ward.SERIAL_NUM = u.FKGD_RESID_REGION3
                       AND gd_ward.ENTRY_STATUS = '1'

    /* ===== BRANCH CATEGORY LOOKUP ===== */
         LEFT JOIN GENERIC_DETAIL gd_category
                   ON gd_category.FK_GENERIC_HEADPAR = u.FKGH_HAS_UNIT_CATE
                       AND gd_category.SERIAL_NUM = u.FKGD_HAS_UNIT_CATE
                       AND gd_category.ENTRY_STATUS = '1'

    /* ===== BANKING SERVICES LOOKUP (D104) ===== */
         LEFT JOIN GENERIC_DETAIL gd_services
                   ON gd_services.FK_GENERIC_HEADPAR = 'D104'
                       AND gd_services.SERIAL_NUM = 1 -- Default service type
                       AND gd_services.ENTRY_STATUS = '1'

    /* ===== MOBILE MONEY SERVICES LOOKUP (D70) ===== */
         LEFT JOIN GENERIC_DETAIL gd_mobile
                   ON gd_mobile.FK_GENERIC_HEADPAR = 'D70'
                       AND gd_mobile.SERIAL_NUM = 1 -- Default mobile service
                       AND gd_mobile.ENTRY_STATUS = '1'

    /* ===== BUSINESS LICENSE LOOKUP ===== */
         LEFT JOIN GENERIC_DETAIL gd_license
                   ON gd_license.FK_GENERIC_HEADPAR = 'LICNS' -- Business license type
                       AND gd_license.SERIAL_NUM = 1
                       AND gd_license.ENTRY_STATUS = '1'

    /* ===== FSR CODE LOOKUP ===== */
         LEFT JOIN GENERIC_DETAIL gd_fsr
                   ON gd_fsr.FK_GENERIC_HEADPAR = 'FSRCD' -- FSR code type
                       AND gd_fsr.SERIAL_NUM = u.CODE
                       AND gd_fsr.ENTRY_STATUS = '1'

WHERE
   -- Only active units (branches)
    u.UNIT_NAME = 'MLIMANI BRANCH'
   OR u.UNIT_NAME = 'SAMORA BRANCH'
-- Exclude non-branch units if any
ORDER BY u.CODE;
-- Premises Furniture Equipment Data Mapping Query
-- This query maps data from PROFITS database tables to the premisesFurnitureEquipment BOT reporting format
-- Uses ASSET_MASTER, ASSET_HCATEGORIES, and related tables

SELECT 
    -- Reporting date & time (mandatory)
    CURRENT_TIMESTAMP AS reportingDate,
    
    -- Asset Category (mandatory) - See Attribute Table D100 for lookup values
    CASE 
        WHEN am.CATEGORY_CODE IN ('01', '02', '03') THEN 'Fixed' -- Buildings, Land, Infrastructure
        WHEN am.CATEGORY_CODE IN ('04', '05', '06', '07', '08', '09') THEN 'Movable' -- Furniture, Equipment, Vehicles, IT
        ELSE 'Fixed' -- Default to Fixed
    END AS assetCategory,
    
    -- Usage of Premises Furniture And Equipment (mandatory) - See Attribute Table D101 for lookup values
    CASE 
        WHEN UPPER(am.DESCRIPTION) LIKE '%OFFICE%' OR UPPER(am.DESCRIPTION) LIKE '%ADMIN%' THEN 'Administrative'
        WHEN UPPER(am.DESCRIPTION) LIKE '%BRANCH%' OR UPPER(am.DESCRIPTION) LIKE '%CUSTOMER%' THEN 'Operational'
        WHEN UPPER(am.DESCRIPTION) LIKE '%ATM%' OR UPPER(am.DESCRIPTION) LIKE '%BANKING%' THEN 'Banking Operations'
        WHEN UPPER(am.DESCRIPTION) LIKE '%IT%' OR UPPER(am.DESCRIPTION) LIKE '%COMPUTER%' THEN 'Information Technology'
        WHEN UPPER(am.DESCRIPTION) LIKE '%SECURITY%' THEN 'Security'
        ELSE 'Administrative' -- Default
    END AS usagePremisesFurnitureEquipment,
    
    -- Acquisition date (mandatory)
    COALESCE(am.AQUISITION_DATE, am.TRX_DATE) AS acquisitionDate,
    
    -- Asset Type (mandatory) - See Attribute Table D30 for lookup values
    CASE 
        WHEN am.CATEGORY_CODE = '01' THEN 'Buildings'
        WHEN am.CATEGORY_CODE = '02' THEN 'Land'
        WHEN am.CATEGORY_CODE = '03' THEN 'Infrastructure'
        WHEN am.CATEGORY_CODE = '04' THEN 'Furniture'
        WHEN am.CATEGORY_CODE = '05' THEN 'Office Equipment'
        WHEN am.CATEGORY_CODE = '06' THEN 'Vehicles'
        WHEN am.CATEGORY_CODE = '07' THEN 'IT Equipment'
        WHEN am.CATEGORY_CODE = '08' THEN 'Security Equipment'
        WHEN am.CATEGORY_CODE = '09' THEN 'Other Equipment'
        ELSE COALESCE(ac.DESCRIPTION, 'Other Equipment')
    END AS assetType,
    
    -- Currency (mandatory) - See Currency table for lookup values
    CASE 
        WHEN am.CURRENCY_ID = 22 THEN 'TZS' -- Tanzanian Shilling
        WHEN am.CURRENCY_ID = 1 THEN 'USD' -- US Dollar
        WHEN am.CURRENCY_ID = 2 THEN 'EUR' -- Euro
        ELSE 'TZS' -- Default to TZS
    END AS currency,
    
    -- Original Amount (mandatory) - The original value of the assets
    COALESCE(am.AMOUNT, am.OBJ_VALUE, 0) AS orgAmount,
    
    -- TZS Amount (mandatory) - Value in Tanzanian Shillings
    CASE 
        WHEN am.CURRENCY_ID = 22 THEN COALESCE(am.AMOUNT, am.OBJ_VALUE, 0) -- Already in TZS
        ELSE COALESCE(am.AMOUNT, am.OBJ_VALUE, 0) * 2300 -- Convert to TZS (approximate rate)
    END AS tzsAmount,
    
    -- USD Equivalent Amount (mandatory) - Value in USD
    CASE 
        WHEN am.CURRENCY_ID = 1 THEN COALESCE(am.AMOUNT, am.OBJ_VALUE, 0) -- Already in USD
        WHEN am.CURRENCY_ID = 22 THEN COALESCE(am.AMOUNT, am.OBJ_VALUE, 0) / 2300 -- Convert from TZS
        ELSE COALESCE(am.AMOUNT, am.OBJ_VALUE, 0) -- Assume USD equivalent
    END AS usdAmount,
    
    -- Disposal Date (optional) - Date when asset was disposed
    CASE 
        WHEN am.ASSET_STATUS = 'D' OR am.ASSET_STATUS = 'S' THEN am.TRX_LDATE -- Disposed or Sold
        ELSE NULL
    END AS disposalDate,
    
    -- Original Value of asset disposed (optional)
    CASE 
        WHEN am.ASSET_STATUS = 'D' OR am.ASSET_STATUS = 'S' THEN COALESCE(am.AMOUNT, am.OBJ_VALUE, 0)
        ELSE NULL
    END AS orgAssetDisposedValue,
    
    -- USD Value of asset disposed (optional)
    CASE 
        WHEN am.ASSET_STATUS = 'D' OR am.ASSET_STATUS = 'S' THEN 
            CASE 
                WHEN am.CURRENCY_ID = 1 THEN COALESCE(am.AMOUNT, am.OBJ_VALUE, 0)
                WHEN am.CURRENCY_ID = 22 THEN COALESCE(am.AMOUNT, am.OBJ_VALUE, 0) / 2300
                ELSE COALESCE(am.AMOUNT, am.OBJ_VALUE, 0)
            END
        ELSE NULL
    END AS usdAssetDisposedValue,
    
    -- TZS Value of asset disposed (optional)
    CASE 
        WHEN am.ASSET_STATUS = 'D' OR am.ASSET_STATUS = 'S' THEN 
            CASE 
                WHEN am.CURRENCY_ID = 22 THEN COALESCE(am.AMOUNT, am.OBJ_VALUE, 0)
                ELSE COALESCE(am.AMOUNT, am.OBJ_VALUE, 0) * 2300
            END
        ELSE NULL
    END AS tzsAssetDisposedValue,
    
    -- Original Currency Depreciation Amount (mandatory)
    COALESCE(
        (COALESCE(am.AMOUNT, am.OBJ_VALUE, 0) - COALESCE(am.DEPRECIAT_VAL_REM, am.CURR_VALUE, 0)) - 
        COALESCE(am.DEPRECIATED_VALUE, 0), 0
    ) AS orgDepreciation,
    
    -- USD Equivalent Depreciation (optional)
    CASE 
        WHEN am.CURRENCY_ID = 1 THEN 
            COALESCE((COALESCE(am.AMOUNT, am.OBJ_VALUE, 0) - COALESCE(am.DEPRECIAT_VAL_REM, am.CURR_VALUE, 0)) - 
            COALESCE(am.DEPRECIATED_VALUE, 0), 0)
        WHEN am.CURRENCY_ID = 22 THEN 
            COALESCE((COALESCE(am.AMOUNT, am.OBJ_VALUE, 0) - COALESCE(am.DEPRECIAT_VAL_REM, am.CURR_VALUE, 0)) - 
            COALESCE(am.DEPRECIATED_VALUE, 0), 0) / 2300
        ELSE 
            COALESCE((COALESCE(am.AMOUNT, am.OBJ_VALUE, 0) - COALESCE(am.DEPRECIAT_VAL_REM, am.CURR_VALUE, 0)) - 
            COALESCE(am.DEPRECIATED_VALUE, 0), 0)
    END AS usdDepreciation,
    
    -- TZS Depreciation Amount (mandatory)
    CASE 
        WHEN am.CURRENCY_ID = 22 THEN 
            COALESCE((COALESCE(am.AMOUNT, am.OBJ_VALUE, 0) - COALESCE(am.DEPRECIAT_VAL_REM, am.CURR_VALUE, 0)) - 
            COALESCE(am.DEPRECIATED_VALUE, 0), 0)
        ELSE 
            COALESCE((COALESCE(am.AMOUNT, am.OBJ_VALUE, 0) - COALESCE(am.DEPRECIAT_VAL_REM, am.CURR_VALUE, 0)) - 
            COALESCE(am.DEPRECIATED_VALUE, 0), 0) * 2300
    END AS tzsDepreciation,
    
    -- Original Currency Accumulated Depreciation (mandatory)
    COALESCE(am.DEPRECIATED_VALUE, 0) AS orgAccumDepreciation,
    
    -- USD Equivalent Accumulated Depreciation (mandatory)
    CASE 
        WHEN am.CURRENCY_ID = 1 THEN COALESCE(am.DEPRECIATED_VALUE, 0)
        WHEN am.CURRENCY_ID = 22 THEN COALESCE(am.DEPRECIATED_VALUE, 0) / 2300
        ELSE COALESCE(am.DEPRECIATED_VALUE, 0)
    END AS usdAccumDepreciation,
    
    -- TZS Equivalent Accumulated Depreciation (mandatory)
    CASE 
        WHEN am.CURRENCY_ID = 22 THEN COALESCE(am.DEPRECIATED_VALUE, 0)
        ELSE COALESCE(am.DEPRECIATED_VALUE, 0) * 2300
    END AS tzsAccumDepreciation,
    
    -- Original Currency Impairment Amount (mandatory)
    COALESCE(am.REVAL_PROFIT, 0) AS orgImpairmentAmount,
    
    -- USD Equivalent Impairment Amount (mandatory)
    CASE 
        WHEN am.CURRENCY_ID = 1 THEN COALESCE(am.REVAL_PROFIT, 0)
        WHEN am.CURRENCY_ID = 22 THEN COALESCE(am.REVAL_PROFIT, 0) / 2300
        ELSE COALESCE(am.REVAL_PROFIT, 0)
    END AS usdImpairmentAmount,
    
    -- TZS Impairment Amount (mandatory)
    CASE 
        WHEN am.CURRENCY_ID = 22 THEN COALESCE(am.REVAL_PROFIT, 0)
        ELSE COALESCE(am.REVAL_PROFIT, 0) * 2300
    END AS tzsImpairmentAmount,
    
    -- Original Currency Net Book Value (mandatory)
    COALESCE(am.CURR_VALUE, am.DEPRECIAT_VAL_REM, am.UNDEPRECIATE_ASSET_VALUE, 0) AS orgNetBookValue,
    
    -- USD Net Book Value (optional)
    CASE 
        WHEN am.CURRENCY_ID = 1 THEN COALESCE(am.CURR_VALUE, am.DEPRECIAT_VAL_REM, am.UNDEPRECIATE_ASSET_VALUE, 0)
        WHEN am.CURRENCY_ID = 22 THEN COALESCE(am.CURR_VALUE, am.DEPRECIAT_VAL_REM, am.UNDEPRECIATE_ASSET_VALUE, 0) / 2300
        ELSE COALESCE(am.CURR_VALUE, am.DEPRECIAT_VAL_REM, am.UNDEPRECIATE_ASSET_VALUE, 0)
    END AS usdNetBookValue,
    
    -- TZS Equivalent Net Book Value (mandatory)
    CASE 
        WHEN am.CURRENCY_ID = 22 THEN COALESCE(am.CURR_VALUE, am.DEPRECIAT_VAL_REM, am.UNDEPRECIATE_ASSET_VALUE, 0)
        ELSE COALESCE(am.CURR_VALUE, am.DEPRECIAT_VAL_REM, am.UNDEPRECIATE_ASSET_VALUE, 0) * 2300
    END AS tzsNetBookValue

FROM ASSET_MASTER am
LEFT JOIN ASSET_HCATEGORIES ac ON am.CATEGORY_CODE = ac.CATEGORY_CODE

WHERE 
    -- Filter for active and relevant assets
    am.ASSET_STATUS IS NOT NULL
    AND am.AQUISITION_DATE IS NOT NULL
    AND am.AQUISITION_DATE >= '2010-01-01' -- Assets acquired since 2010
    AND (
        -- Include premises, furniture, and equipment categories
        am.CATEGORY_CODE IN ('01', '02', '03', '04', '05', '06', '07', '08', '09')
        OR UPPER(am.DESCRIPTION) LIKE '%OFFICE%'
        OR UPPER(am.DESCRIPTION) LIKE '%FURNITURE%'
        OR UPPER(am.DESCRIPTION) LIKE '%EQUIPMENT%'
        OR UPPER(am.DESCRIPTION) LIKE '%BUILDING%'
        OR UPPER(am.DESCRIPTION) LIKE '%PREMISES%'
    )

ORDER BY am.AQUISITION_DATE DESC, am.ASSET_ID;
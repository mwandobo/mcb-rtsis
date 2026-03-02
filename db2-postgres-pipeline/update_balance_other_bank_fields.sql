-- Update balance with other banks fields to standard values
UPDATE "balanceWithOtherBank"
SET 
    "subAccountType" = 'Normal',
    "externalRatingCorrespondentBank" = 'Unrated',
    "gradesUnratedBanks" = 'Grade B';

-- Check the update
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT "subAccountType") as distinct_subAccountType,
    COUNT(DISTINCT "externalRatingCorrespondentBank") as distinct_externalRating,
    COUNT(DISTINCT "gradesUnratedBanks") as distinct_grades
FROM "balanceWithOtherBank";

SELECT
    VARCHAR_FORMAT (CURRENT TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
    d.BRANCH_CODE AS branchCode,
    d.DIRECTOR_NAME AS directorName,
    VARCHAR_FORMAT (d.DIRECTOR_DOB, 'DDMMYYYYHHMM') AS directorDob,
    d.DIRECTOR_NIN AS directorNin,
    d.DIRECTOR_CATEGORY AS directorCategory,
    d.DIRECTOR_CLASS AS directorClass,
    VARCHAR_FORMAT (d.APPOINTMENT_DATE, 'DDMMYYYYHHMM') AS appointmentDate,
    d.GENDER AS gender,
    d.COMMITTEE_MEMBERSHIP AS committeeMembership,
    d.DIRECTOR_FEES AS directorFees,
    '[' || COALESCE(
        LISTAGG (
            '{"benefitType":"' || COALESCE(b.BENEFIT_TYPE, '') || '","benefitValue":' || RTRIM (CHAR(b.BENEFIT_VALUE)) || '}',
            ','
        ) WITHIN GROUP (
            ORDER BY
                b.BENEFIT_ID
        ),
        ''
    ) || ']' AS directorBenefits,
    d.DIRECTOR_ALLOWANCE AS directorAllowance
FROM
    BOARD_OF_DIRECTORS d
    LEFT JOIN BOARD_OF_DIRECTORS_BENEFITS b ON b.DIRECTOR_ID = d.DIRECTOR_ID
GROUP BY
    d.DIRECTOR_ID,
    d.BRANCH_CODE,
    d.DIRECTOR_NAME,
    d.DIRECTOR_DOB,
    d.DIRECTOR_NIN,
    d.DIRECTOR_CATEGORY,
    d.DIRECTOR_CLASS,
    d.APPOINTMENT_DATE,
    d.GENDER,
    d.COMMITTEE_MEMBERSHIP,
    d.DIRECTOR_FEES,
    d.DIRECTOR_ALLOWANCE
ORDER BY
    d.DIRECTOR_ID;
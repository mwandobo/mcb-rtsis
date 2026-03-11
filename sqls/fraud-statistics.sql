SELECT
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')      AS reportingDate,

    fs.PRODUCT_PROCESS_INVOLVED                            AS productProcessInvolved,

    VARCHAR_FORMAT(fs.OCCURRENCE_DATE, 'DDMMYYYYHHMM')     AS occurrenceDate,

    VARCHAR_FORMAT(fs.DETECTION_DATE, 'DDMMYYYYHHMM')      AS detectionDate,

    VARCHAR_FORMAT(fs.CONCLUDED_INVESTIGATION_DATE, 'DDMMYYYYHHMM')
                                                           AS concludedInvestigationDate,

    fs.DISCOVERY_MODEL                                     AS discoveryModel,

    VARCHAR_FORMAT(fs.FSP_FRAUD_IDENTIFIED_DATE, 'DDMMYYYYHHMM')
                                                           AS fspFraudIdentifiedDate,

    fs.IDENTIFIED_FRAUD_CAUSES                             AS identifiedFraudCauses,

    fs.FRAUD_AMOUNT                                        AS fraudAmount,

    fs.CLASSIFICATION                                      AS classification,

    fs.FRAUD_TYPE                                          AS fraudType,

    fs.FRAUD_NATURE                                        AS fraudNature,

    fs.FRAUD_DESCRIPTION                                   AS fraudDescription,

    fs.VICTIM_NAME                                         AS victimName,

    fs.ACCOUNT_NUMBER                                      AS accountNumber,

    fs.BRANCH_CODE                                         AS branchCode,

    fs.ACTION_TAKEN                                        AS actionTaken,

    fs.MITIGATION_MEASURES                                 AS mitigationMeasures,

    fs.FRAUD_STATUS                                        AS fraudStatus,

    fs.EMP_NIN                                             AS empNin,

    fs.FRAUDSTER_NAME                                      AS fraudsterName,

    fs.FRAUDSTER_COUNTRY                                   AS fraudsterCountry

FROM FRAUD_STATISTICS fs

ORDER BY fs.OCCURRENCE_DATE DESC
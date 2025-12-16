create table TMP_BS130
(
    DEBTOR                VARCHAR(4000),
    ON_BAL_SHEET          DECIMAL(15, 2),
    OFF_BAL_SHEET         DECIMAL(15, 2),
    TOTAL                 DECIMAL(15, 2),
    CORE                  DECIMAL(15, 2),
    PERC_TOTAL            DECIMAL(15, 2),
    STATUS                VARCHAR(14),
    FINAL_SUB_CLASS       VARCHAR(11),
    COLL_AMOUNT           DECIMAL(15, 2),
    COLL_TYPE             VARCHAR(4000),
    DEBTOR_FINSC          VARCHAR(4000),
    DEBTOR_PROVISION_AMNT DECIMAL(15, 2),
    CURRENCY              CHAR(3)
);


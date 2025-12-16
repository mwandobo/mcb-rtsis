create table TMP_REST1
(
    AGR_MEMBERSHIP_SN  INTEGER,
    AGR_SN             INTEGER,
    AGR_YEAR           SMALLINT,
    FK_UNITCODE        INTEGER,
    CUST_ID            INTEGER,
    SUM_LOAN_AMN1      DECIMAL(15, 2),
    SUM_LOAN_AMN2      DECIMAL(15, 2),
    SUM_LG_AMN         DECIMAL(15, 2),
    REST_AMN1          DECIMAL(15, 2),
    FK_CURRENCYID_CURR INTEGER,
    FIXING             DECIMAL(15, 2),
    AFM_NO             CHAR(20),
    ACTIVITY           CHAR(40),
    GROUP_FLG          DECIMAL(15, 2)
);


create table CUST_STATUS_N3869
(
    AB_CUST_CODE                VARCHAR(20),
    PRFT_CUST_ID                INTEGER,
    UD_CUST_ID                  CHAR(40),
    STATUS_N3869                CHAR(2),
    FLAG_01                     INTEGER,
    FLAG_02                     INTEGER,
    FLAG_03                     INTEGER,
    FLAG_04                     INTEGER,
    APLC_DCSN_REGISTRATION_DT   DATE,
    APPLICATION_DECISION_NUMBER VARCHAR(20),
    APPLICATION_DECISION_DT     DATE,
    MAGISTRATES_COURT_CODE      VARCHAR(50),
    TMSTAMP                     TIMESTAMP(6),
    ROW_STATUS                  INTEGER,
    ROW_ERR_DESC                VARCHAR(500)
);


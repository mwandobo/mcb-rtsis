create table REP_74605
(
    TMSTAMP           TIMESTAMP(6),
    REC_SN            INTEGER,
    PROD_CATEGORY     INTEGER,
    ZIP_CODE          CHAR(10),
    CUST_ID           INTEGER,
    AGR_UNIT          INTEGER,
    AGR_YEAR          SMALLINT,
    AGR_SN            INTEGER,
    AGR_MEMBERSHIP_SN SMALLINT,
    ACC_UNIT          INTEGER,
    ACC_TYPE          SMALLINT,
    ACC_SN            INTEGER,
    BENEF_GUAR_FLG    CHAR(1),
    CUST_DESC         CHAR(40),
    ADDRESS_1         CHAR(40),
    CITY              CHAR(30),
    ID_PRODUCT        INTEGER,
    PROD_DESC         CHAR(40),
    MAIN_BENEF_DESC   CHAR(40)
);

create unique index IXU_LOA_061
    on REP_74605 (TMSTAMP, REC_SN, PROD_CATEGORY, ZIP_CODE);


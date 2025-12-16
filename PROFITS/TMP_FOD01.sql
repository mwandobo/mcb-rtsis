create table TMP_FOD01
(
    AGR_YEAR           SMALLINT,
    FK_UNITCODE        INTEGER,
    AGR_SN             INTEGER,
    AGR_MEMBERSHIP_SN  INTEGER,
    FKCUR_IS_MOVED_IN  INTEGER,
    CUST_ID            INTEGER,
    NRM_BALANCE        DECIMAL(15, 2),
    FK_INTERESTID_INTE DECIMAL(15, 2),
    ID_PRODUCT         DECIMAL(15, 2),
    OV_BALANCE         DECIMAL(15, 2),
    ACC_OPEN_DT        DATE,
    AGR_ISSUE_DT       DATE,
    ACC_EXP_DT         DATE,
    IBAN               CHAR(1),
    AGREEMENT_NUMBER   CHAR(40),
    AFM_NO             VARCHAR(20),
    FATHER_NAME        VARCHAR(20),
    MOTHER_NAME        VARCHAR(20),
    ID_NO              VARCHAR(20),
    NAME               VARCHAR(92),
    ADDRESS            VARCHAR(114)
);

create unique index IX_TMP_FOD01
    on TMP_FOD01 (FK_UNITCODE, AGR_MEMBERSHIP_SN, AGR_SN, AGR_YEAR, CUST_ID);


create table TMP_BOG_REPORT1
(
    GROUP_FLG          SMALLINT,
    ACC_TYPE           SMALLINT,
    FK_UNITCODE        INTEGER,
    CURRENCY_ID        INTEGER,
    ACC_SN             INTEGER,
    CUST_ID            INTEGER,
    FIXING             DECIMAL(12, 6),
    REST_AMN1          DECIMAL(15, 2),
    EXCEPT_AMN2        DECIMAL(15, 2),
    EXCEPT_AMN1        DECIMAL(15, 2),
    REST_AMN3          DECIMAL(15, 2),
    LOAN_AMN1          DECIMAL(15, 2),
    LOAN_AMN2          DECIMAL(15, 2),
    LG_AMN             DECIMAL(15, 2),
    REST_AMN2          DECIMAL(15, 2),
    FIRST_NAME         CHAR(20),
    AFM_NO             CHAR(20),
    SURNAME            CHAR(70),
    ACTIVITY           VARCHAR(40),
    FK_AGREEMENTAGR_ME INTEGER,
    FK_AGREEMENTAGR_SN INTEGER,
    FK_AGREEMENTAGR_YE SMALLINT,
    FK_AGREEMENTFK_UNI INTEGER
);

create unique index IX_TMP_BOG_REPORT1_CUSTID
    on TMP_BOG_REPORT1 (CUST_ID);


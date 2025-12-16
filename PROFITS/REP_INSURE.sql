create table REP_INSURE
(
    ACC_CD         SMALLINT,
    ACC_SN         INTEGER,
    ACC_TYPE       SMALLINT,
    ACC_UNIT       INTEGER,
    TRX_DATE       DATE,
    CUST_ID        INTEGER,
    AMOUNT         DECIMAL(15, 2),
    EXP_DATE       DATE,
    FROM_DATE      DATE,
    TO_DATE        DATE,
    CURRENCY       CHAR(5),
    ZIP            CHAR(10),
    CITY           CHAR(30),
    ADDRESS        CHAR(40),
    INSUR_CONTR_NO CHAR(40),
    NAME0          CHAR(90)
);

create unique index IXU_REP_007
    on REP_INSURE (ACC_CD, ACC_SN, ACC_TYPE, ACC_UNIT, TRX_DATE);


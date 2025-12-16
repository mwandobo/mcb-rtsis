create table REP_COMPUCARE
(
    ACC_UNIT    INTEGER  not null,
    ACC_TYPE    SMALLINT not null,
    ACC_SN      INTEGER  not null,
    LOAN_STATUS CHAR(1),
    ACC_CD      SMALLINT,
    GL_ACCOUNT  CHAR(21),
    BALANCE_AMN DECIMAL(15, 2),
    ID_CURRENCY INTEGER,
    CUST_ID     INTEGER,
    ID_PRODUCT  INTEGER,
    CCODE_ID    INTEGER,
    constraint PKCOMCAR
        primary key (ACC_UNIT, ACC_TYPE, ACC_SN)
);


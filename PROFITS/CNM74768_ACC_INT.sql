create table CNM74768_ACC_INT
(
    CREATION_DT       DATE     not null,
    ACCOUNT_NUMBER    CHAR(16) not null,
    ACC_CD            SMALLINT,
    ACC_SN            INTEGER,
    ACC_TYPE          SMALLINT,
    UNIT_CODE         INTEGER,
    INSTALL_FIXED_AMN DECIMAL(15, 2),
    INSTALL_CNT       SMALLINT,
    INSTALL_FREQUENCY SMALLINT,
    constraint PIX_74768
        primary key (ACCOUNT_NUMBER, CREATION_DT)
);


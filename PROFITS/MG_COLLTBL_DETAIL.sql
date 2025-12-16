create table MG_COLLTBL_DETAIL
(
    FILE_NAME        CHAR(50) not null,
    SERIAL_NO        INTEGER  not null,
    FILE_DETAIL_ID   CHAR(2),
    COLLATERAL_SN    CHAR(40) not null,
    REAL_ESTATE_ID   CHAR(40) not null,
    VOLUME           VARCHAR(40),
    SHEET            VARCHAR(40),
    LAWYER           VARCHAR(40),
    PRENOTATION_AMN  DECIMAL(15, 2),
    MAIN_CONNECT_IND CHAR(1),
    REMOVAL_IND      CHAR(1),
    COMMENTS1        CHAR(80),
    COMMENTS2        CHAR(80),
    COMMENTS3        CHAR(80),
    REMOVAL_DATE     DATE,
    PARAM_RECRS      CHAR(30) not null,
    PARAM_CARRE      CHAR(30) not null,
    PARAM_LANRG      CHAR(30),
    ROW_STATUS       CHAR(1),
    constraint MG_COLTBL_DTL
        primary key (SERIAL_NO, FILE_NAME)
);


create table TMP_TEKE_MNT_REC
(
    TRX_DATE          DATE         not null,
    TRX_UNIT          INTEGER      not null,
    TRX_USER          CHAR(8)      not null,
    TRX_USR_SN        INTEGER      not null,
    EMP_ID            CHAR(8),
    LAST_NAME         VARCHAR(20),
    FIRST_NAME        VARCHAR(20),
    GRP_SUBSCRIPT     SMALLINT     not null,
    PARAMETER_TYPE    CHAR(10),
    TRX_CODE          VARCHAR(10)  not null,
    TRX_DETAILS_SN    VARCHAR(15)  not null,
    TRX_DETAILS_DESC  VARCHAR(40)  not null,
    TRX_DETAILS_SDESC VARCHAR(10)  not null,
    TMSTAMP           TIMESTAMP(6) not null,
    constraint PK_TMP_TEKE_MNT_REC
        primary key (TRX_DATE, TRX_UNIT, TRX_USER, TRX_USR_SN, GRP_SUBSCRIPT)
);


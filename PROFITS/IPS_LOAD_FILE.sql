create table IPS_LOAD_FILE
(
    TRX_DATE            DATE     not null,
    FILENAME            CHAR(50) not null,
    LINE_NO             INTEGER  not null,
    FULL_LINE           VARCHAR(2048),
    SETUP_ID            CHAR(20),
    ID_JUSTIFIC         INTEGER,
    TMSTAMP             TIMESTAMP(6),
    ORDER_CODE          VARCHAR(20),
    SETUP_JUSTIFIC      INTEGER,
    FIELD_SECTION       SMALLINT,
    COMPLETE_FLAG       CHAR(1),
    DATA_ROW_SN         DECIMAL(10),
    GROUP_ID            DECIMAL(10),
    INTERBANK_SETTLE_DT DATE,
    constraint IXU_CP__58
        primary key (TRX_DATE, LINE_NO, FILENAME)
);

create unique index IXN_IPS_LOAD_FILE_01
    on IPS_LOAD_FILE (FILENAME, TRX_DATE);


create table IPS_TEMPLATE_DETAIL
(
    TEMPLATE_ID        INTEGER  not null,
    DATA_SN            SMALLINT not null,
    DATA_NAME          CHAR(50) not null,
    DATA_LABEL         VARCHAR(50),
    DATA_TYPE          SMALLINT,
    DATA_FILE_COLUMN   INTEGER,
    DATA_DEFAULT_VALUE VARCHAR(100),
    constraint PK_IPS_TEMPLATE_DETAIL
        primary key (TEMPLATE_ID, DATA_SN)
);


create table PROJECT_MASTER
(
    PROJECT_ID          DECIMAL(5)   not null
        constraint IXU_PROJECT_MAS
            primary key,
    PROJECT_DESCRIPTION VARCHAR(200) not null,
    PROJECT_KIND        DECIMAL(5)   not null,
    PROJECT_TYPE        DECIMAL(5)   not null,
    START_DATE          DATE,
    END_DATE            DATE,
    PROJECT_AMOUNT      DECIMAL(18, 2),
    FK_CURRENCY_ID      DECIMAL(5),
    RESPONSIBLE         VARCHAR(200),
    STATUS              VARCHAR(1),
    COMMENTS            VARCHAR(500),
    TRX_DATE            DATE         not null,
    TRX_UNIT            DECIMAL(5)   not null,
    TRX_USER            CHAR(8)      not null,
    TMSTAMP             TIMESTAMP(6)
);


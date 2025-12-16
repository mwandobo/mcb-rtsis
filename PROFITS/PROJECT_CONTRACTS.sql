create table PROJECT_CONTRACTS
(
    CONTRACT_ID          DECIMAL(5)    not null,
    FK_PROJECT_ID        DECIMAL(5)    not null,
    CONTRACT_DESCRIPTION VARCHAR(200)  not null,
    CONTRACT_KIND        DECIMAL(5)    not null,
    START_DATE           DATE,
    END_DATE             DATE,
    STATUS               VARCHAR(1),
    CONTENT              BLOB(1048576) not null,
    FILE_NAM             VARCHAR(255)  not null,
    COMMENTS             VARCHAR(500),
    TRX_DATE             DATE          not null,
    TRX_UNIT             DECIMAL(5)    not null,
    TRX_USER             CHAR(8)       not null,
    TMSTAMP              TIMESTAMP(6),
    constraint IXU_PROJECT_CON
        primary key (CONTRACT_ID, FK_PROJECT_ID)
);


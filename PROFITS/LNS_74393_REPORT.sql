create table LNS_74393_REPORT
(
    SCHEDULED_DATE    DATE,
    LNS_UNIT          DECIMAL(5),
    ACCOUNT_NUMBER    CHAR(40) not null
        constraint PK_LNS_74393_REPORT
            primary key,
    ACCOUNT_CD        DECIMAL(2),
    PROCESSED_BY_FLOW CHAR(1),
    FOR_GENERATION    CHAR(1),
    GENERATED         CHAR(1),
    DATE_FROM         DATE,
    DATE_TO           DATE,
    FILENAME          CHAR(70),
    ERROR_DESCRIPTION CHAR(80),
    INSERT_TIMESTAMP  TIMESTAMP(6)
);


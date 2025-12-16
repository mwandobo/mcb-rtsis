create table TPROJECT5
(
    PID           CHAR(11) not null
        constraint IXU_PRD_025
            primary key,
    AT_HOME       SMALLINT,
    REGION_ID     SMALLINT,
    TOTAL_AMOUNT  DECIMAL(8, 2),
    ACCEPT_AMOUNT DECIMAL(8, 2),
    BIRTH_DATE    DATE,
    MFO           CHAR(3),
    ACCOUNT       CHAR(34),
    ZIP_CODE      VARCHAR(4),
    FID           VARCHAR(12),
    CITY          VARCHAR(50),
    VILLAGE       VARCHAR(50),
    DISTRICT      VARCHAR(50),
    FIRST_NAME    VARCHAR(50),
    LAST_NAME     VARCHAR(50),
    REGION        VARCHAR(50),
    GOVERNMENT    VARCHAR(50),
    FULL_ADDRESS  VARCHAR(255)
);


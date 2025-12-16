create table TPROJECT3
(
    PID          CHAR(11) not null
        constraint IXU_PRD_024
            primary key,
    REGION_ID    SMALLINT,
    TOTAL_AMOUNT DECIMAL(8, 2),
    BIRTH_DATE   DATE,
    MFO          CHAR(9),
    ACCOUNT      CHAR(34),
    ZIP_CODE     VARCHAR(4),
    FIRST_NAME   VARCHAR(100),
    VILLAGE      VARCHAR(100),
    DISTRICT     VARCHAR(100),
    CITY         VARCHAR(100),
    LAST_NAME    VARCHAR(100),
    ADDRESS      VARCHAR(200)
);


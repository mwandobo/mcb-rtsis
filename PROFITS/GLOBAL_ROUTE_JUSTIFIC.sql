create table GLOBAL_ROUTE_JUSTIFIC
(
    SN                   DECIMAL(10) not null
        constraint PK_GLOBAL_JUSTIFIC
            primary key,
    DB_CURRENCY          INTEGER,
    CR_CURRENCY          INTEGER,
    DIRECTION            CHAR(1),
    SAME_DAY_VALUE       CHAR(1),
    DB_JUSTIFIC          INTEGER,
    CR_JUSTIFIC          INTEGER,
    FOREIGN_CR_CURRENCY  CHAR(1),
    FOREIGN_DB_CURRENCY  CHAR(1),
    DOMESTIC_CR_CURRENCY CHAR(1),
    DOMESTIC_DB_CURRENCY CHAR(1),
    ALL_CR_CURRENCY      CHAR(1),
    ALL_DB_CURRENCY      CHAR(1),
    ALL_CHARGES          CHAR(1),
    ALL_ROUTES           CHAR(1),
    CREATE_UNIT          INTEGER,
    CREATE_DATE          DATE,
    CREATE_USR           CHAR(8),
    CREATE_TMSTAMP       TIMESTAMP(6),
    UPDATE_UNIT          INTEGER,
    UPDATE_DATE          DATE,
    UPDATE_USR           CHAR(8),
    UPDATE_TMSTAMP       TIMESTAMP(6),
    FK_GH_BCHRG          CHAR(5),
    FK_GD_BCHRG          INTEGER,
    FK_GH_ROUT3          CHAR(5),
    FK_GD_ROUT3          INTEGER,
    ENTRY_STATUS         CHAR(1),
    DIFFERENT_CURRENCY   CHAR(1)
);


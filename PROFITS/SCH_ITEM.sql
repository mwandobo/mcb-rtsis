create table SCH_ITEM
(
    FK_SCRIPT          VARCHAR(40)          not null,
    ID                 VARCHAR(40)          not null,
    NAME               VARCHAR(100)         not null,
    ENABLED            SMALLINT   default 0 not null,
    EMAIL              VARCHAR(100),
    PARENT             VARCHAR(40)          not null,
    BREAK              SMALLINT   default 0,
    ITEMTYPE           SMALLINT   default 0,
    JOBTYPE            SMALLINT   default 0,
    DESCRIPTION        VARCHAR(2000)        not null,
    CRITICAL           SMALLINT   default 0,
    PROGRAMID          VARCHAR(5),
    CHECKDATE          VARCHAR(15),
    PARAMETERS         CLOB(1048576),
    POSITION           SMALLINT   default 0 not null,
    TRANSFERTYPE       SMALLINT   default 0,
    BACKUPSOURCE       SMALLINT   default 0,
    ACTIVATION_QUERY   CLOB(1048576),
    EMBEDDED_SCRIPT_ID VARCHAR(40),
    AUTO_BATCH_RESET   DECIMAL(1) default 0 not null,
    CHECK_QUERY        CLOB(1048576),
    constraint SCH_ITEM_PK
        primary key (FK_SCRIPT, ID)
);


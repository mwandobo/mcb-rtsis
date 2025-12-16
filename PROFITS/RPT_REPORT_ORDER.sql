create table RPT_REPORT_ORDER
(
    ID                    INTEGER            not null
        constraint RPT_REPORT_ORDER_PK
            primary key,
    FK_REPORT_ID          INTEGER            not null,
    STATUS                SMALLINT default 0 not null,
    CREATED               TIMESTAMP(6)       not null,
    CREATED_BY            VARCHAR(50)        not null,
    FK_LANGUAGE_ID        INTEGER,
    PROTECT               SMALLINT default 1,
    FK_DATABASE_ID        INTEGER  default 0 not null,
    CRITERIA_HASH         CHAR(24),
    ORDER_COUNTER         INTEGER  default 1 not null,
    STATUS_ORDER          SMALLINT default 0 not null,
    USER_STATUS_ORDER     VARCHAR(50),
    STATUS_ORDER_DATETIME TIMESTAMP(6),
    FREE_TEXT             VARCHAR(200),
    FK_REPORT_RESULT_ID   INTEGER
);


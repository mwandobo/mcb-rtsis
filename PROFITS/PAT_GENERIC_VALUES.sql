create table PAT_GENERIC_VALUES
(
    VALUE_ID                       DECIMAL(10) not null
        constraint PATGVPK1
            primary key,
    STATIC_DATA                    CHAR(100),
    FK_PAT_GENVALUEGVT_ID          CHAR(5),
    FK_PAT_TEST_INSFK_PAT_ACTIONST INTEGER,
    FK_PAT_TEST_INSINSTRUCTION_ID  DECIMAL(10),
    ARRAY_INDEX                    SMALLINT,
    USAGE                          CHAR(1),
    NAME                           CHAR(32),
    FK_PAT_TEST_MANID              CHAR(10),
    SCOPE                          CHAR(1)
);

create unique index PATGVI2
    on PAT_GENERIC_VALUES (FK_PAT_GENVALUEGVT_ID);

create unique index PATGVI3
    on PAT_GENERIC_VALUES (FK_PAT_TEST_INSINSTRUCTION_ID, FK_PAT_TEST_INSFK_PAT_ACTIONST);

create unique index PATGVI4
    on PAT_GENERIC_VALUES (FK_PAT_TEST_MANID);


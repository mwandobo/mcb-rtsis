create table XPAT_GENERIC_VALUE
(
    VALUE_ID                      DECIMAL(10) not null
        constraint PATXGVPK
            primary key,
    STATIC_DATA                   CHAR(100),
    ARRAY_INDEX                   SMALLINT,
    USAGE0                        CHAR(1),
    NAME0                         CHAR(32),
    FK_XPAT_GENVALUGVT_ID         CHAR(5),
    FK_XPAT_TEST_ININSTRUCTION_ID DECIMAL(10),
    FK_XPAT_USER_PRID             CHAR(10),
    SCOPE                         CHAR(1)
);

comment on column XPAT_GENERIC_VALUE.ARRAY_INDEX is 'This attribute defines which value will be used in the test execution repeating group';

comment on column XPAT_GENERIC_VALUE.USAGE0 is 'The attribute that defines how the value of default type FXENT going to be used:C - common_constant means constant common for all tests independently of TESTER, i.e. single value used by any USER_NAME who runs the test, and any Bank, for which the tests';

comment on column XPAT_GENERIC_VALUE.NAME0 is 'When value is treated as variable, this attribute contains this variable name. The variable names in turn can be referenced by other instructions, for example PASTE';

create unique index PATXGVI1
    on XPAT_GENERIC_VALUE (FK_XPAT_USER_PRID);

create unique index PATXGVI2
    on XPAT_GENERIC_VALUE (FK_XPAT_TEST_ININSTRUCTION_ID);

create unique index PATXGVI3
    on XPAT_GENERIC_VALUE (FK_XPAT_GENVALUGVT_ID);


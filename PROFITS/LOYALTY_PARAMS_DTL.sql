create table LOYALTY_PARAMS_DTL
(
    TAG_SET_CODE CHAR(20)       not null,
    TAG          CHAR(10)       not null,
    VALUE_FROM   DECIMAL(15, 2) not null,
    VALUE_TO     DECIMAL(15, 2) not null,
    LOYAL_POINTS INTEGER,
    ENTRY_STATUS CHAR(1),
    constraint LOYAL_DTL_PK
        primary key (TAG_SET_CODE, VALUE_FROM, VALUE_TO, TAG)
);


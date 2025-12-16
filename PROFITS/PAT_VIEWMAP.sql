create table PAT_VIEWMAP
(
    ENCY_IN_OUT_TYPE    CHAR(1)      not null,
    ENCY_ALIAS_NAME     CHAR(32)     not null,
    STATUS_FLAG         CHAR(1)      not null,
    LAST_CHANGED        TIMESTAMP(6) not null,
    VC_ENTITY_ID        DECIMAL(10),
    VC_ATTRIBUTE_ID     DECIMAL(10),
    FK_PAT_WINCONTRUID0 DECIMAL(10)  not null
        constraint PATVIPK1
            primary key
);


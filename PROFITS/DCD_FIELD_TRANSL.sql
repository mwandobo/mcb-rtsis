create table DCD_FIELD_TRANSL
(
    LANGUAGE_USED      INTEGER  not null,
    TABLE_ATTRIBUTE    CHAR(40) not null,
    TABLE_ENTITY       CHAR(40) not null,
    DESCRIPTION        CHAR(40),
    FUNCTIONALITY_DESC CHAR(240),
    constraint IXU_DEF_046
        primary key (LANGUAGE_USED, TABLE_ATTRIBUTE, TABLE_ENTITY)
);


create table DCD_TRNS_FLD_TRASL
(
    CODE               CHAR(8)      not null,
    LANGUAGE_USED      INTEGER      not null,
    TABLE_ATTRIBUTE    CHAR(40)     not null,
    TABLE_ENTITY       CHAR(40)     not null,
    TMPSTAMP           TIMESTAMP(6) not null,
    STATUS0            CHAR(1),
    PASSWORD           CHAR(26),
    DESCRIPTION        CHAR(40),
    FUNCTIONALITY_DESC CHAR(240),
    constraint IXU_DEF_067
        primary key (CODE, LANGUAGE_USED, TABLE_ATTRIBUTE, TABLE_ENTITY, TMPSTAMP)
);


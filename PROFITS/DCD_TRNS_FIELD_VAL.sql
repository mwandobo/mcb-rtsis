create table DCD_TRNS_FIELD_VAL
(
    CODE               CHAR(8)      not null,
    PERMITTED_VALUE_ID DECIMAL(12)  not null,
    TABLE_ATTRIBUTE    CHAR(40)     not null,
    TABLE_ENTITY       CHAR(40)     not null,
    TMPSTAMP           TIMESTAMP(6) not null,
    STATUS0            CHAR(1),
    PASSWORD           CHAR(26),
    LOW_VALUE          CHAR(150),
    HIGH_VALUE         CHAR(150),
    VALUE_DESC         VARCHAR(2048),
    constraint IXU_DEF_066
        primary key (CODE, PERMITTED_VALUE_ID, TABLE_ATTRIBUTE, TABLE_ENTITY, TMPSTAMP)
);


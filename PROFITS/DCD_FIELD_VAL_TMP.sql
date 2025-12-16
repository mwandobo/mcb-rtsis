create table DCD_FIELD_VAL_TMP
(
    PERMITTED_VALUE_ID DECIMAL(12) not null,
    NUM_OCCUR          INTEGER     not null,
    TABLE_ENTITY       CHAR(40)    not null,
    TABLE_ATTRIBUTE    CHAR(40)    not null,
    LOW_VALUE          CHAR(150),
    HIGH_VALUE         CHAR(150),
    VALUE_DESC         VARCHAR(2048),
    constraint IXU_DEF_115
        primary key (PERMITTED_VALUE_ID, NUM_OCCUR, TABLE_ENTITY, TABLE_ATTRIBUTE)
);


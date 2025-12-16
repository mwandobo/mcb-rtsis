create table DCD_FIELD_VALUES
(
    PERMITTED_VALUE_ID DECIMAL(12) not null,
    TABLE_ATTRIBUTE    CHAR(40)    not null,
    TABLE_ENTITY       CHAR(40)    not null,
    LOW_VALUE          CHAR(150),
    HIGH_VALUE         CHAR(150),
    VALUE_DESC         VARCHAR(2048),
    constraint IXU_DEF_001
        primary key (PERMITTED_VALUE_ID, TABLE_ATTRIBUTE, TABLE_ENTITY)
);


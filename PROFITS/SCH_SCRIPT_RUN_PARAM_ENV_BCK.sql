create table SCH_SCRIPT_RUN_PARAM_ENV_BCK
(
    TIMESTAMP_BCK           TIMESTAMP(6)         not null,
    USER_BCK                VARCHAR(20)          not null,
    FK_SCH_SCRIPT_RUN_PARAM VARCHAR(40)          not null,
    KEY                     VARCHAR(100)         not null,
    VALUE                   VARCHAR(4000)        not null,
    POSITION                SMALLINT,
    OVERRIDE                DECIMAL(1) default 0 not null
);


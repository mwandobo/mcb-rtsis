create table SCH_SCRIPT_RUN_PARAM_ENV_SSN
(
    SESSION_ID              TIMESTAMP(6)         not null,
    FK_SCH_SCRIPT_RUN_PARAM VARCHAR(40)          not null,
    KEY                     VARCHAR(100)         not null,
    VALUE                   VARCHAR(4000)        not null,
    POSITION                SMALLINT,
    OVERRIDE                DECIMAL(1) default 0 not null
);


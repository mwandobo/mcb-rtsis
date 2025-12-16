create table SCH_SCRIPT_RUN_PARAM_ENV
(
    FK_SCH_SCRIPT_RUN_PARAM VARCHAR(40)          not null,
    KEY                     VARCHAR(100)         not null,
    VALUE                   VARCHAR(4000)        not null,
    POSITION                SMALLINT,
    OVERRIDE                DECIMAL(1) default 0 not null,
    constraint SCH_SCRIPT_RUN_PARAM_ENV_VALPK
        primary key (FK_SCH_SCRIPT_RUN_PARAM, KEY)
);


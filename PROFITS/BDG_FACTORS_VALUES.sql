create table BDG_FACTORS_VALUES
(
    FK_BDG_FACTORS_ID CHAR(3)  not null,
    YEAR0             SMALLINT not null,
    TYPE              CHAR(1)  not null,
    STATUS            SMALLINT,
    FTH_VAL           DECIMAL(15, 11),
    THD_VAL           DECIMAL(15, 11),
    FST_VAL           DECIMAL(15, 11),
    SEC_VAL           DECIMAL(15, 11),
    TMSTMP            DATE,
    constraint IXU_GL_024
        primary key (FK_BDG_FACTORS_ID, YEAR0, TYPE)
);


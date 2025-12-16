create table PAY_ROLL_REVERSAL
(
    TIMESTMP                       TIMESTAMP(6) not null,
    PIG_REC_SN                     INTEGER      not null,
    PIG_FILE_SN                    DECIMAL(10)  not null,
    FK0FX_FT_RECORDTUN_INTERNAL_SN SMALLINT,
    FK_FX_FT_RECORDTUN_INTERNAL_SN SMALLINT,
    FK0FX_FT_RECORDTRX_UNIT        INTEGER,
    FK_FX_FT_RECORDTRX_UNIT        INTEGER,
    FK0FX_FT_RECORDTRX_SN          INTEGER,
    FK_FX_FT_RECORDTRX_SN          INTEGER,
    NEW_LIMIT                      DECIMAL(15, 2),
    OLD_LIMIT                      DECIMAL(15, 2),
    FK0FX_FT_RECORDTRX_DATE        DATE,
    FK_FX_FT_RECORDTRX_DATE        DATE,
    ENTRY_STATUS                   CHAR(1),
    FK0FX_FT_RECORDTRX_USR         CHAR(8),
    FK_FX_FT_RECORDTRX_USR         CHAR(8),
    constraint IXU_PRD_012
        primary key (TIMESTMP, PIG_REC_SN, PIG_FILE_SN)
);


create table LOAN_FIXED_INTER_SCALE
(
    ACC_TYPE           SMALLINT not null,
    ACC_SN             INTEGER  not null,
    ACC_UNIT           INTEGER  not null,
    ID_INTEREST        INTEGER  not null,
    TRX_DATE           DATE     not null,
    NEW_FX_INT_EXP_DT  DATE     not null,
    INT_DURATION       SMALLINT,
    OLD_FX_INT_ST_DT   DATE,
    OLD_FX_INT_EXP_DT  DATE,
    OLD_FX_INT_SCAL_DT DATE,
    NEW_FX_INT_ST_DT   DATE,
    NEW_FX_INT_SCAL_DT DATE,
    OLD_PERCENTAGE     DECIMAL(9, 6),
    NEW_PERCENTAGE     DECIMAL(9, 6),
    TMSTAMP            TIMESTAMP(6),
    VALEUR_DT          DATE,
    TRX_USR            CHAR(8),
    NEW_PERCENT_IND    CHAR(1),
    NEW_DAYBASE        SMALLINT,
    constraint PK_LFIS
        primary key (NEW_FX_INT_EXP_DT, TRX_DATE, ID_INTEREST, ACC_UNIT, ACC_SN, ACC_TYPE)
);


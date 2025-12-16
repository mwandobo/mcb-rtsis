create table LOAN_ACC_MG_EXTRAIT
(
    REQUEST_LOAN_STS CHAR(1)  not null,
    REQUEST_SN       SMALLINT not null,
    REQUEST_TYPE     CHAR(1)  not null,
    ACC_SN           INTEGER  not null,
    ACC_TYPE         SMALLINT not null,
    ACC_UNIT         INTEGER  not null,
    ACR_PNL_INT_AMN  DECIMAL(15, 2),
    ACR_NRM_INT_AMN  DECIMAL(15, 2),
    URL_PNL_INT_AMN  DECIMAL(15, 2),
    URL_NRM_INT_AMN  DECIMAL(15, 2),
    RL_NRM_INT_AMN   DECIMAL(15, 2),
    RL_PNL_INT_AMN   DECIMAL(15, 2),
    TRX_DATE         DATE,
    RQ_EXPIRE_DT     DATE,
    RQ_CREATION_DT   DATE,
    VALEUR_DT        DATE,
    ENTRY_STATUS     CHAR(1),
    constraint IXU_MIG_037
        primary key (REQUEST_LOAN_STS, REQUEST_SN, REQUEST_TYPE, ACC_SN, ACC_TYPE, ACC_UNIT)
);


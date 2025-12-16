create table LNS_REQ_CR_DB
(
    PACKET_ID          INTEGER not null,
    INTERNAL_PACKET_SN INTEGER not null,
    ACC_TYPE           SMALLINT,
    ACC_CD             SMALLINT,
    REQUEST_SN         SMALLINT,
    FK_UNITCODE        INTEGER,
    UNITCODE           INTEGER,
    ID_CURRENCY        INTEGER,
    ID_JUSTIFIC        INTEGER,
    ID_TRANSACT        INTEGER,
    ACC_SN             INTEGER,
    IN_TRX_URL_PNL_INT DECIMAL(15, 2),
    IN_TRX_RL_PNL_INT  DECIMAL(15, 2),
    IN_TRX_RL_NRM_INT  DECIMAL(15, 2),
    IN_TRX_CAPITAL     DECIMAL(15, 2),
    IN_TRX_URL_NRM_INT DECIMAL(15, 2),
    IN_TRX_EXP         DECIMAL(15, 2),
    IN_TRX_COM         DECIMAL(15, 2),
    VALEUR_DT          DATE,
    REQUEST_LOAN_STS   CHAR(1),
    REQUEST_TYPE       CHAR(1),
    TRANSACTION_FLG    CHAR(1),
    REQUEST_STS        CHAR(1),
    USRCODE            CHAR(8),
    I_TRX_COMMENTS     CHAR(40),
    ERROR_MESSAGE      CHAR(80),
    constraint IXU_LNS_052
        primary key (PACKET_ID, INTERNAL_PACKET_SN)
);


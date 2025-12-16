create table PROOF_GL_AGREEMENT
(
    TRX_DATE        DATE    not null,
    TRX_UNIT        INTEGER not null,
    TRX_USR         CHAR(8) not null,
    TRX_CURRENCY_ID INTEGER not null,
    DIFFERENCE      DECIMAL(15, 2),
    USRTOT_AMOUNT   DECIMAL(15, 2),
    GL_AMOUNT       DECIMAL(15, 2),
    GL_ACCOUNTACC   CHAR(21),
    constraint IXU_FX_024
        primary key (TRX_DATE, TRX_UNIT, TRX_USR, TRX_CURRENCY_ID)
);


create table GLG_NOTANNOUNCED
(
    TIMESTAMP         TIMESTAMP(6) not null,
    LC_SIGN           CHAR(1),
    SIGN_AMOUNT       CHAR(1),
    DEAL_SLIP_NUM     DECIMAL(10)  not null,
    UNIT_CODE         CHAR(5)      not null,
    TYPE_CODE         CHAR(2),
    SERVICE_TYPE      CHAR(2),
    GL_ACCOUNT        CHAR(21),
    LC_AMOUNT         DECIMAL(15, 2),
    AMOUNT            DECIMAL(15, 2),
    PAYED_CURRENCY    INTEGER      not null,
    RECEIVED_CURRENCY INTEGER      not null,
    VALEUR_DATE       DATE         not null,
    TRN_DATE          DATE         not null,
    WORK_TYPE         CHAR(2)      not null,
    constraint PK_GLG_NOTANNOUNCED
        primary key (WORK_TYPE, TRN_DATE, VALEUR_DATE, RECEIVED_CURRENCY, PAYED_CURRENCY, UNIT_CODE, DEAL_SLIP_NUM,
                     TIMESTAMP)
);


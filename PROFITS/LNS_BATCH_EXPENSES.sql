create table LNS_BATCH_EXPENSES
(
    FK_JUSTIFICID      INTEGER  not null,
    FK_PRFT_TRANSACID  INTEGER  not null,
    FK_LOAN_ACCOUNTFK  INTEGER  not null,
    FK_LOAN_ACCOUNTACC INTEGER  not null,
    FK0LOAN_ACCOUNTACC SMALLINT not null,
    CALC_DT            DATE     not null,
    EXPENSES_TYPE      CHAR(2)  not null,
    EXPENSES_AMNT      DECIMAL(15, 2),
    TMPSTAMP           TIMESTAMP(6),
    constraint IXU_LNS_032
        primary key (FK_JUSTIFICID, FK_PRFT_TRANSACID, FK_LOAN_ACCOUNTFK, FK_LOAN_ACCOUNTACC, FK0LOAN_ACCOUNTACC,
                     CALC_DT, EXPENSES_TYPE)
);


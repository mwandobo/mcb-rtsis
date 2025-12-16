create table VALIDITY_DATE
(
    VALIDITY_DATE                  DATE,
    ENTRY_STATUS                   CHAR(1),
    HOME_BRANCH                    CHAR(1),
    PROFIT_INDICATOR               CHAR(1),
    TMSTAMP                        TIMESTAMP(6),
    FK_AGREEMENT_TYFK_PRODUCTID_PR INTEGER not null,
    FK_JUSTIFICID_JUSTIFIC         INTEGER not null,
    FK_PRFT_TRANSACID_TRANSACT     INTEGER not null,
    constraint PKAGRTRX
        primary key (FK_PRFT_TRANSACID_TRANSACT, FK_JUSTIFICID_JUSTIFIC, FK_AGREEMENT_TYFK_PRODUCTID_PR)
);


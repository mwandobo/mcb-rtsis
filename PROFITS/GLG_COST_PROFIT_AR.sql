create table GLG_COST_PROFIT_AR
(
    TRN_ID             CHAR(6),
    TMSTAMP            TIMESTAMP(6),
    COST_TYPE_FLAG     CHAR(1),
    ENTRY_STATUS       CHAR(1),
    FK_GLG_ACCOUNT_FRO CHAR(21),
    FK0GLG_ACCOUNT_TO  CHAR(21),
    DESCRIPTION        CHAR(30)
);

create unique index IXU_GLG_009
    on GLG_COST_PROFIT_AR (TRN_ID);


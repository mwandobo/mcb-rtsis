create table TRACCOUNT_COBENEF
(
    FK_TRACCOUNT_UNITC INTEGER      not null,
    FK_TRACCOUNT_TYPE  SMALLINT     not null,
    FK_TRACCOUNTACC_SN INTEGER      not null,
    FK_CUSTOMERCUST_ID INTEGER      not null,
    TMSTAMP            TIMESTAMP(6) not null,
    REMOVAL_DT         DATE,
    MAIN_BENEF_FLG     CHAR(1),
    AEDAK_FLG          CHAR(1),
    BENEF_STATUS       CHAR(1),
    constraint IXU_TRA_007
        primary key (FK_TRACCOUNT_UNITC, FK_TRACCOUNT_TYPE, FK_TRACCOUNTACC_SN, FK_CUSTOMERCUST_ID, TMSTAMP)
);


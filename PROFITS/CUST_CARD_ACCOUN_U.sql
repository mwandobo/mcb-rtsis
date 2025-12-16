create table CUST_CARD_ACCOUN_U
(
    FK_CUST_CARD_INCAR CHAR(16)     not null,
    ACCOUNT_NO         CHAR(20)     not null,
    ACCOUNT_TYPE       CHAR(1)      not null,
    START_DATE         DATE,
    END_DATE           DATE,
    TMSTAMP            TIMESTAMP(6) not null,
    ENTRY_STATUS       CHAR(1),
    constraint IXU_CIU_023
        primary key (ACCOUNT_NO, FK_CUST_CARD_INCAR)
);


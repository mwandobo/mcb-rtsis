create table SPLIT_UP_BANKING_U
(
    FK_OTHER_BANKFK_GE CHAR(5),
    FK_OTHER_BANKFK_G1 INTEGER,
    FK_OTHER_BANKDISCR CHAR(15)     not null,
    FK_CUSTOMERCUST_ID INTEGER      not null,
    SERIAL_NUM         SMALLINT     not null,
    BRANCH             VARCHAR(30),
    ENTRY_COMMENTS     VARCHAR(30),
    CREDIT_LIMIT       DECIMAL(15, 2),
    ENTRY_STATUS       CHAR(1)      not null,
    TMSTAMP            TIMESTAMP(6) not null,
    constraint IXU_CIU_056
        primary key (SERIAL_NUM, FK_CUSTOMERCUST_ID, FK_OTHER_BANKDISCR)
);


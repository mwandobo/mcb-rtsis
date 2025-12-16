create table CUST_CARD_INFO_U
(
    FK_CUSTOMERCUST_ID INTEGER      not null,
    CARD_NO            CHAR(16)     not null
        constraint IXU_CIU_024
            primary key,
    TMSTAMP            TIMESTAMP(6) not null,
    START_DATE         DATE,
    END_DATE           DATE,
    ENTRY_STATUS       CHAR(1),
    CARD_COMNTS        VARCHAR(30)
);


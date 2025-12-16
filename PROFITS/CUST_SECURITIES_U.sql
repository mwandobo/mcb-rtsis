create table CUST_SECURITIES_U
(
    FK_CUSTOMERCUST_ID INTEGER not null
        constraint IXU_CIU_036
            primary key,
    AEDAK_FLG          CHAR(1),
    TMSTAMP            TIMESTAMP(6),
    INSTITUTIONAL      CHAR(1),
    PARTY_IND          CHAR(1),
    PORTFOLIO_OWNER    CHAR(1),
    CUST_SPECIFICATION CHAR(1)
);


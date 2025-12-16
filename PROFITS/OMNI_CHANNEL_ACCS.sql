create table OMNI_CHANNEL_ACCS
(
    ID_CHANNEL      INTEGER     not null,
    OMNI_USER_ID    DECIMAL(15) not null,
    ACCOUNT_NUMBER  CHAR(40)    not null,
    PRFT_SYSTEM     SMALLINT    not null,
    OMNI_USER_NAME  VARCHAR(40),
    INDIVID_CUST_ID DECIMAL(15),
    CORPOR_CUST_ID  DECIMAL(15),
    STATUS          CHAR(1),
    TMSTAMP         TIMESTAMP(6),
    constraint PK_OMNI_CHANNEL_AC
        primary key (ID_CHANNEL, PRFT_SYSTEM, ACCOUNT_NUMBER, OMNI_USER_ID)
);


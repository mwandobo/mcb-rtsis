create table CUST_ACC_EXTRN_CHN
(
    CUST_ID          INTEGER  not null,
    ID_CHANNEL       INTEGER  not null,
    ACCOUNT_NUMBER   CHAR(40) not null,
    ACCOUNT_CD       SMALLINT,
    PRFT_SYSTEM      SMALLINT not null,
    REGISTRATION_ID  CHAR(40) not null,
    EXPIRATION_DATE  DATE     not null,
    STATUS           CHAR(1),
    TMSTAMP          TIMESTAMP(6),
    LAST_UPDATE_USER CHAR(8),
    LAST_UPDATE_DATE DATE,
    GENERIC_BIT      CHAR(5),
    GENERIC_NUM      DECIMAL(15, 2),
    FK_CHANTYPE_HEAD CHAR(5)  not null,
    FK_CHANTYPE_DET  INTEGER  not null,
    constraint PK_CUSTACCCHN
        primary key (FK_CHANTYPE_HEAD, FK_CHANTYPE_DET, PRFT_SYSTEM, REGISTRATION_ID, ACCOUNT_NUMBER, ID_CHANNEL,
                     CUST_ID)
);


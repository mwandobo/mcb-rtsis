create table CUST_ACC_CHANNEL_LIMITS
(
    CARD_NUMBER           CHAR(20),
    DESCRIPTION           CHAR(40),
    ALL_ACCOUNT_FLG       CHAR(1)             not null,
    SN                    DECIMAL(10),
    PRFT_SYSTEM           SMALLINT            not null,
    ACCOUNT_NMBR          CHAR(40)            not null,
    C_DIGIT               SMALLINT,
    CUST_ID               INTEGER             not null,
    ENTRY_STATUS          CHAR(1),
    DISSALOW_FLG          CHAR(1),
    LIMIT_TYPE            CHAR(3),
    CYCLE_TYPE            CHAR(1),
    CYCLE_NUMBER          INTEGER,
    EXPIRATION_DT         DATE,
    SPECIFIC_CHANNEL      INTEGER             not null,
    ALL_CHANNEL_INDICATOR CHAR(1)             not null,
    LIMIT_AMOUNT          DECIMAL(15, 2)      not null,
    UTILIZED_AMOUNT       DECIMAL(15, 2),
    AVAILABLE_AMOUNT      DECIMAL(15, 2),
    ALL_TRN_INDICATOR     CHAR(1) default '0' not null,
    GRP_TRN               CHAR(8) default '0' not null,
    NON_DEFAULT           CHAR(1),
    PER_TRANSACTION       CHAR(1),
    constraint PK_CHNL_LMT
        primary key (ALL_CHANNEL_INDICATOR, SPECIFIC_CHANNEL, CUST_ID, ACCOUNT_NMBR, PRFT_SYSTEM, ALL_ACCOUNT_FLG,
                     ALL_TRN_INDICATOR, GRP_TRN)
);


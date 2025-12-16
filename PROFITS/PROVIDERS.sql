create table PROVIDERS
(
    PROVIDER_ID       DECIMAL(11) not null
        constraint IXU_CP_068
            primary key,
    CONNECT_STATUS    SMALLINT,
    SENDTIME          DECIMAL(11),
    STATUS            DECIMAL(11),
    SHORT_DESCRIPTION CHAR(8),
    DATA_TABLE        CHAR(10),
    MFO               CHAR(16),
    DD_TRANZ_MFO      CHAR(16),
    MODULEIP          CHAR(16),
    DD_TRANZ_ACCOUNT  CHAR(24),
    ACCOUNT_NUMBER    CHAR(24),
    IDENTIFY_MASK     CHAR(32),
    DESCRIPTION       CHAR(50),
    RULS              CHAR(100),
    MODULEPORT        CHAR(128),
    MODULE_NAME       CHAR(254),
    ISSUE_TIME        TIME
);


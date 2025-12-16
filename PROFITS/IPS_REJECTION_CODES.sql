create table IPS_REJECTION_CODES
(
    GROUP_CODE      CHAR(5)     not null,
    CODE            VARCHAR(10) not null,
    USAGE           VARCHAR(30) not null,
    DESCRIPTION     VARCHAR(50),
    RECALL_FLAG     CHAR(1),
    STP_RECALL_FLAG CHAR(1),
    UNPAID_FLAG     CHAR(1),
    UNAPPLIED_FLAG  CHAR(1),
    REASON_POS      VARCHAR(20),
    REASON_PURP     VARCHAR(20),
    constraint IXU_CP__62
        primary key (USAGE, CODE, GROUP_CODE)
);


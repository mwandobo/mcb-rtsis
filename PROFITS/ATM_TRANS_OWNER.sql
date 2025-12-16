create table ATM_TRANS_OWNER
(
    BIN_ACQ_FLG      CHAR(1)     not null,
    TRANS_OWNER      VARCHAR(16) not null,
    FORCE_POST_FLAG  CHAR(1)     not null,
    TRANSACTION_CODE DECIMAL(10) not null,
    ID_JUSTIFIC      INTEGER,
    ID_JUSTIFIC2     INTEGER,
    constraint IXU_ATM_040
        primary key (TRANSACTION_CODE, FORCE_POST_FLAG, BIN_ACQ_FLG, TRANS_OWNER)
);


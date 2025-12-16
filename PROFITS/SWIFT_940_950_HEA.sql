create table SWIFT_940_950_HEA
(
    FK_SWIFT_PRFT_NO        CHAR(16) not null,
    SEQ_YEAR                SMALLINT,
    CUST_ID                 INTEGER,
    ENTRY_STATUS            CHAR(1),
    PRIORITY_CODE           CHAR(1),
    STATE_SEQ_NO_28C        CHAR(11),
    TRX_REF_NO_21           CHAR(16),
    TRX_REF_NO_20           CHAR(16),
    FW_AV_BAL_65            CHAR(30),
    CLOS_AV_BAL_64          CHAR(30),
    CLOS_BAL_62A            CHAR(30),
    OPEN_BAL_60A            CHAR(30),
    ACC_IDENT_25            CHAR(35),
    INFORM_ACC_OW_86        VARCHAR(390),
    STATE_SEQ_NO_28C_PART_1 DECIMAL(15),
    STATE_SEQ_NO_28C_PART_2 DECIMAL(15)
);

create unique index IXU_FX_059
    on SWIFT_940_950_HEA (FK_SWIFT_PRFT_NO, ACC_IDENT_25, STATE_SEQ_NO_28C);

create unique index SWIFT_940_950_UNQ_IDX
    on SWIFT_940_950_HEA (ACC_IDENT_25, SEQ_YEAR, STATE_SEQ_NO_28C, CUST_ID);


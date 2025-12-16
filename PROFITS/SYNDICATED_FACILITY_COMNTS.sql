create table SYNDICATED_FACILITY_COMNTS
(
    FACILITY_ID         DECIMAL(10) not null,
    FACILITY_CMNT_SN    DECIMAL(10) not null,
    FACILITY_CMNT_VALUE VARCHAR(250),
    ENTRY_STATUS        CHAR(1),
    TRX_USER            CHAR(8),
    TRX_DATE            DATE,
    TRX_TIMESTAMP       TIMESTAMP(6),
    constraint ISYNDICATED_FACILITY_CMNTS
        primary key (FACILITY_ID, FACILITY_CMNT_SN)
);


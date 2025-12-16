create table CUTTOFF_TIMES
(
    TRX_CODE    INTEGER     not null,
    AGR_NO      DECIMAL(15) not null,
    CHANNEL_ID  INTEGER     not null,
    CUTOFF_TIME TIME,
    constraint CUTOFF_TIMES_PK
        primary key (TRX_CODE, AGR_NO, CHANNEL_ID)
);


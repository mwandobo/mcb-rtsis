create table CPM_LOANS_PAY_PROCESSED
(
    LIQUIDATOR           SMALLINT not null,
    ACC_UNIT             SMALLINT not null,
    ACC_TYPE             SMALLINT not null,
    ACC_SN               INTEGER  not null,
    ACC_CD               SMALLINT not null,
    BIG_CUST_CODE        INTEGER  not null,
    SERIAL_NO            INTEGER  not null,
    CREATION_DT          DATE     not null,
    INTERNAL_SN          INTEGER  not null,
    PRFT_PROCESS_STATUS  CHAR(1),
    PRFT_PROCESS_TMSTAMP TIMESTAMP(6),
    QUALCO_UPDATED       CHAR(1),
    constraint ICPM_LOANS_PAY_PROCESSED
        primary key (LIQUIDATOR, ACC_UNIT, ACC_TYPE, ACC_SN, ACC_CD, BIG_CUST_CODE, SERIAL_NO, CREATION_DT, INTERNAL_SN)
);


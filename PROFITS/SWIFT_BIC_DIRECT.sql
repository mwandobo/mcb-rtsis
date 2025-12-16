create table SWIFT_BIC_DIRECT
(
    SWIFT_BIC    CHAR(11) not null,
    BRANCH_CODE  CHAR(3)  not null,
    ENTRY_STATUS CHAR(1)  not null,
    MODIFLAG     CHAR(1),
    BANK_NAME    CHAR(105),
    BANK_CITY    CHAR(35),
    REC_DATE     DATE,
    REC_TMSTAMP  TIMESTAMP(6),
    constraint SWFBICPK
        primary key (SWIFT_BIC, BRANCH_CODE)
);


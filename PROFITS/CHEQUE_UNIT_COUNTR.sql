create table CHEQUE_UNIT_COUNTR
(
    FK_UNITCODE INTEGER,
    CNTR        DECIMAL(10),
    TMSTAMP     DATE
);

create unique index IXU_CHE_004
    on CHEQUE_UNIT_COUNTR (FK_UNITCODE);


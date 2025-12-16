create table BILL_INV_SEGM
(
    MASSEG_SERIAL_NUM  DECIMAL(10)  not null
        constraint IXU_BIL_32
            primary key,
    BILL_CR_ACC_NUMBER CHAR(40),
    BILL_CR_ACC_CD     SMALLINT,
    BILL_CRACC_PRFSYS  SMALLINT,
    CRACC_AMOUNT       DECIMAL(15, 2),
    TMSTAMP            TIMESTAMP(6) not null,
    FK_MASS_REG_SERIAL DECIMAL(10)
);

create unique index IXN_BIL_37
    on BILL_INV_SEGM (FK_MASS_REG_SERIAL);


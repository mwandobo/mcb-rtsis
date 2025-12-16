create table SUP_TRANSACTIONS_H
(
    SUP_ID_TRANSACT  DECIMAL(5) not null
        constraint IXU_SUP_0001
            primary key,
    SUP_VALUE_DB     DECIMAL(1) default '0',
    SUP_VALUE_CR     DECIMAL(1) default '0',
    SUP_VALUE_INV_DB DECIMAL(1) default '0',
    SUP_VALUE_INV_CR DECIMAL(1) default '0',
    PURCHASES_STS    DECIMAL(1) default '0',
    PAYMENTS_STS     DECIMAL(1) default '0',
    OTHERS_STS       DECIMAL(1) default '0',
    TRX_LUNIT        DECIMAL(5) default '0',
    TRX_UNIT         DECIMAL(5) default '0',
    TRX_LDATE        DATE,
    TRX_DATE         DATE,
    TRX_LUSR         CHAR(8),
    TRX_USR          CHAR(8),
    STATUS_CODE      CHAR(4)    default '0',
    COMMENTS         CHAR(200)
);


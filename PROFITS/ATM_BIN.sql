create table ATM_BIN
(
    BIN_CODE      CHAR(9),
    JUSTIFIC_1    INTEGER,
    JUSTIFIC_2    INTEGER,
    BIN_TYPE      CHAR(1),
    BANK_IND      CHAR(1),
    DIAS_CODE     CHAR(3),
    ACCOUNT_NO    CHAR(40),
    CARDLESS_FLAG VARCHAR(1)
);

comment on column ATM_BIN.CARDLESS_FLAG is 'Indicates whether it is used for Cardless transactions or not. Values are 1 for Cardless, 0 Otherwise.';

create unique index IXU_ATM_003
    on ATM_BIN (BIN_CODE);


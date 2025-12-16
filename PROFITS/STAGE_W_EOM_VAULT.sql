create table STAGE_W_EOM_VAULT
(
    EOM_DATE        DATE       not null,
    CURRENCY_ID     DECIMAL(5) not null,
    UNIT_CODE       DECIMAL(5) not null,
    TRX_CODE        DECIMAL(5) not null,
    CURRENCY_CODE   CHAR(5),
    AMOUNT          DECIMAL(15),
    TRX_DESCRIPTION VARCHAR(40),
    VAULT_CHARGE    DECIMAL(18, 2),
    VAULT_DISCHARGE DECIMAL(18, 2)
);


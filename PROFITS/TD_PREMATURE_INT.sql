create table TD_PREMATURE_INT
(
    I_TUN_INTERNAL_SN  SMALLINT not null,
    I_TRX_USR_SN       INTEGER  not null,
    I_TRX_USR          CHAR(8)  not null,
    I_TRX_DATE         DATE     not null,
    I_TRX_UNIT         INTEGER  not null,
    FK_TIME_DEPOS_RREN INTEGER,
    FK_TIME_DEPOS_RFK  DECIMAL(11),
    ACC_AMOUNT_35      DECIMAL(15, 2),
    O_VALUE_DATE       DATE,
    ENTRY_STATUS       CHAR(1),
    constraint IXU_DEP_158
        primary key (I_TUN_INTERNAL_SN, I_TRX_USR_SN, I_TRX_USR, I_TRX_DATE, I_TRX_UNIT)
);


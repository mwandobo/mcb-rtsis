create table LOAN_ACC_DISCOUNT
(
    TRX_DATE          DATE     not null,
    ACC_UNIT          INTEGER  not null,
    ACC_TYPE          SMALLINT not null,
    ACC_SN            INTEGER  not null,
    PARTICIPATION_FLG CHAR(1),
    constraint PK_LNSDSCNT
        primary key (ACC_SN, ACC_TYPE, ACC_UNIT, TRX_DATE)
);


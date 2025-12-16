create table EXCEPTION_74191
(
    UNIT_CODE    INTEGER  not null,
    ACC_TYPE     SMALLINT not null,
    ACC_SN       INTEGER  not null,
    TRX_DATE     DATE     not null,
    ACC_CD       SMALLINT,
    EXC_COMMENTS CHAR(40) not null,
    constraint I0000841
        primary key (TRX_DATE, ACC_SN, ACC_TYPE, UNIT_CODE)
);


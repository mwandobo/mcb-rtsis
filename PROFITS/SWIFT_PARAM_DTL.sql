create table SWIFT_PARAM_DTL
(
    SWT_PARAM_DTL_SN INTEGER  not null,
    SWT_VALUE_TYPE   SMALLINT not null,
    VALUE_TEXT       CHAR(20),
    FK_SWT_PARAMETER CHAR(10) not null,
    FK_SWT_PARAM_SN  INTEGER  not null,
    VALUE_TEXT1      CHAR(20),
    VALUE_TEXT2      CHAR(20),
    VALUE_NUM        DECIMAL(18),
    VALUE_NUM1       DECIMAL(18),
    VALUE_NUM2       DECIMAL(18),
    DESCRIPTION      CHAR(50),
    ID_PRODUCT       DECIMAL(5),
    ID_JUSTIFIC      DECIMAL(5),
    ID_TRANSACT      DECIMAL(5),
    constraint PK_SWIFT_PARAM_DTL
        primary key (FK_SWT_PARAMETER, FK_SWT_PARAM_SN, SWT_PARAM_DTL_SN)
);

comment on column SWIFT_PARAM_DTL.SWT_VALUE_TYPE is '1-Message type2-Tag20';

comment on column SWIFT_PARAM_DTL.VALUE_TEXT is 'MESSAGE_TYPEMT103, MT202 etc*** for all message types';


create table SWIFT_PARAMETERS
(
    SWT_PARAMETER    CHAR(10) not null,
    SWT_PARAM_SN     INTEGER  not null,
    SWT_PARAM_DTL    CHAR(1),
    MSG_CATEGORY     CHAR(1),
    FIN_COPY         CHAR(3),
    VALUE_DAYS       SMALLINT,
    CURRENCY         CHAR(5),
    SWIFT_ORIGIN     CHAR(1),
    RECEIVER_BIC     CHAR(11),
    RECEIVER_BIC_EQ  CHAR(1),
    MESSAGE_TYPE     CHAR(20),
    MESSAGE_TYPE_EQ  CHAR(1),
    TAG20_SYS_IND    VARCHAR(5),
    TAG20_SYS_IND_EQ CHAR(1),
    constraint PK_SWIFT_PARAM
        primary key (SWT_PARAMETER, SWT_PARAM_SN)
);

comment on column SWIFT_PARAMETERS.SWT_PARAMETER is 'Permitted Values :BATCH_SEND (Batch production of swift files to be send to Swift Alliance)';

comment on column SWIFT_PARAMETERS.SWT_PARAM_DTL is 'Indicates if parameter details exist.';

comment on column SWIFT_PARAMETERS.MSG_CATEGORY is 'SWIFT Message Category :1. Outgoing2. Incoming3. Sogecash Order';

comment on column SWIFT_PARAMETERS.FIN_COPY is 'Three characters indicating the Fin Copy value of the Swift service';

comment on column SWIFT_PARAMETERS.VALUE_DAYS is 'Select Swifts N days before the Swift value date';

comment on column SWIFT_PARAMETERS.CURRENCY is 'It is a short description of the currency.Normally is associated with the value days field';

comment on column SWIFT_PARAMETERS.SWIFT_ORIGIN is '1-Originated from incoming SWIFT rejection';

comment on column SWIFT_PARAMETERS.RECEIVER_BIC is 'Receiver SWIFT Address';

comment on column SWIFT_PARAMETERS.RECEIVER_BIC_EQ is '1-Equal0-Not EqualMust have a value if Reciever BIC has value';

comment on column SWIFT_PARAMETERS.MESSAGE_TYPE is 'MESSAGE_TYPEMT103, MT202 etc*** for all message types';

comment on column SWIFT_PARAMETERS.MESSAGE_TYPE_EQ is '1-Equal0-Not EqualMust have a value if Message_type has value';

comment on column SWIFT_PARAMETERS.TAG20_SYS_IND is 'SEC for SecuritiesMM for Money MarketFX for FX deals';

comment on column SWIFT_PARAMETERS.TAG20_SYS_IND_EQ is '1-Equal0-Not EqualMust have a value if tag20 indicator has value';


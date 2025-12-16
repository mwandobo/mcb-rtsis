create table HIST_IPS_MESSAGE_FIELDS
(
    ORDER_CODE      VARCHAR(20)  not null,
    TMSTAMP         TIMESTAMP(6) not null,
    FIELD_NAME      VARCHAR(10)  not null,
    FIELD_SN        DECIMAL(10)  not null,
    VALUE_TYPE      CHAR(2),
    VALUE_TEXT      VARCHAR(150),
    VALUE_DATE      DATE,
    VALUE_NUMBER    DECIMAL(15, 2),
    VALUE_TIMESTAMP TIMESTAMP(6),
    VALUE_FLAG      VARCHAR(2),
    FORMATTED_VALUE CHAR(150),
    SUB_FIELD_SN    SMALLINT,
    VALUE_TIME      TIME,
    constraint IXU_HFLD_01
        primary key (ORDER_CODE, FIELD_SN, TMSTAMP)
);


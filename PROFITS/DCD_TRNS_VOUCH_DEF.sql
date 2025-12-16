create table DCD_TRNS_VOUCH_DEF
(
    CODE            CHAR(8)      not null,
    CUST_LANGUAGE   INTEGER      not null,
    TMPSTAMP        TIMESTAMP(6) not null,
    VOUCHER_COLUMN  SMALLINT     not null,
    VOUCHER_ID      DECIMAL(12)  not null,
    VOUCHER_ROW     SMALLINT     not null,
    VAR_LIT_IND     SMALLINT,
    DISPLAY_LENGTH  SMALLINT,
    STATUS          CHAR(1),
    FIELD_TYPE      CHAR(2),
    PASSWORD        CHAR(26),
    FORMAT_NAME     CHAR(30),
    TABLE_ENTITY    CHAR(40),
    TABLE_ATTRIBUTE CHAR(40),
    VOUCHER_DATA    VARCHAR(200),
    constraint IXU_DEF_079
        primary key (CODE, CUST_LANGUAGE, TMPSTAMP, VOUCHER_COLUMN, VOUCHER_ID, VOUCHER_ROW)
);


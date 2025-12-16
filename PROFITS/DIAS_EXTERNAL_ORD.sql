create table DIAS_EXTERNAL_ORD
(
    LINE_SN           INTEGER not null,
    FILE_NAME         CHAR(8) not null,
    FILE_SN           CHAR(2) not null,
    FILE_DATE         DATE    not null,
    ORGANISATION_CODE INTEGER not null,
    EXT_FILE_NUM      DECIMAL(10),
    CREATE_DT         DATE,
    RECORD_TYPE       CHAR(1),
    RECEIVER_CODE     CHAR(3),
    TRANSACT_SET_CODE CHAR(1),
    CURRENCY_CODE     CHAR(3),
    ITEM_COUNT        CHAR(5),
    AMOUNT_COUNT      CHAR(15),
    EXT_ORDER_NUM     DECIMAL(15),
    SENDER_REFERENCE  CHAR(20),
    OUT_REFERENCE     DECIMAL(15),
    TRANS_CODE        CHAR(3),
    DETAIL_OF_CHARGE  CHAR(3),
    TRANS_AMNT        CHAR(15),
    REJECT_CODE       CHAR(3),
    REJECT_REASON     CHAR(40),
    ENTRY_STATUS      CHAR(1),
    TMSTAMP           TIMESTAMP(6),
    DETAIL_LINE       VARCHAR(1800),
    constraint I0000779
        primary key (ORGANISATION_CODE, FILE_DATE, FILE_SN, FILE_NAME, LINE_SN)
);


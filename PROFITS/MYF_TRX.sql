create table MYF_TRX
(
    ID              DECIMAL(15),
    SALES_PURC_IND  SMALLINT,
    TP_TYPE         CHAR(5),
    TP_NUMBER       CHAR(15),
    TAX_REG_NO      CHAR(9),
    PROFESSION      CHAR(15),
    DESCRIPTION     CHAR(27),
    MYF_LIABLE_FLAG CHAR(1),
    CITY_ADDR       CHAR(10),
    STREET_ADDR     CHAR(16),
    STREET_NR_ADDR  CHAR(3),
    POST_CODE       INTEGER,
    INVOICE_NR      CHAR(12),
    ISSUE_DATE      DATE,
    AMNT            DECIMAL(13, 2),
    GOVERMENT_IND   SMALLINT,
    TMSTAMP         TIMESTAMP(6),
    TRX_INTERNAL_SN SMALLINT,
    TP_TASK         CHAR(8)
);


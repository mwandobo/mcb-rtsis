create table GLG_DOCUMENT
(
    DOC_ID             CHAR(4),
    DOC_SER            CHAR(2),
    FK_GD_HAS_SUBSYS   INTEGER,
    DEACTIVATION_DATE  DATE,
    TIMESTMP           TIMESTAMP(6),
    CURRENCY_IND       CHAR(1),
    DOC_TYPE           CHAR(1),
    ON_LINE_BATCH      CHAR(1),
    TMP_BALSH_IND      CHAR(1),
    STATUS             CHAR(1),
    FC_BUY_SELL_FLAG   CHAR(1),
    CHANGE_STATUS_FLAG CHAR(1),
    CUST_FLAG          CHAR(1),
    FK_GLG_JOURNALJOUR CHAR(2),
    CANCEL_DOC_SER     CHAR(2),
    CANCEL_DOC_ID      CHAR(4),
    FK_GH_HAS_SUBSYS   CHAR(5),
    SPECIFIC_USAGE     VARCHAR(2),
    SHORT_DESCR        VARCHAR(15),
    DESCRIPTION        VARCHAR(40)
);

create unique index IXU_GLG_063
    on GLG_DOCUMENT (DOC_ID, DOC_SER);


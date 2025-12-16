create table HFXFT_PARAMETERS
(
    DEF_SWIFT_ADDR     CHAR(40),
    ORD_VAL_DT_SPREAD  SMALLINT     not null,
    BASE_CONV_CURR     INTEGER,
    BEN_CUST_AGE       CHAR(1),
    CNTRY_ISO_CODE     CHAR(2),
    INCL_52A           CHAR(1),
    SEC_CURR_BASE      INTEGER,
    DIV_AMNT           INTEGER,
    DIV_AMNT2          INTEGER,
    TARGET_SYSTEM      CHAR(1),
    FX_ID_PRODUCT      INTEGER,
    FX_ID_JUSTIFIC     INTEGER,
    FXLC_ID_JUSTIFIC   INTEGER,
    LCFX_ID_JUSTIFIC   INTEGER,
    COLLAB_CUST_AGR_OV CHAR(1),
    TRX_CUST_AGE       CHAR(1),
    DEALER_LIM_RULE_ID INTEGER,
    TMSTAMP            TIMESTAMP(6) not null,
    USR_UPD            CHAR(8),
    UPD_DTE            DATE,
    constraint PK_HFXFT
        primary key (TMSTAMP, ORD_VAL_DT_SPREAD)
);


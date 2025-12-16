create table DCD_GH_WINDOW
(
    FK_DCD_PRODUCTID_P         INTEGER  not null,
    FK_DCD_TRANSACTGUI         INTEGER  not null,
    FK_DCD_JUSTIFICID          INTEGER  not null,
    OTHER_GUI_TYPE             SMALLINT not null,
    OTHER_GUI_VALUE            CHAR(5)  not null,
    FK_DCD_RULEPRFT_SY         SMALLINT,
    FK_HPRODUCTID_PRODUCT      INTEGER,
    FK_JUSTIFICID_JUSTIFIC     INTEGER,
    FK_PRFT_TRANSACID_TRANSACT INTEGER,
    FK_DCD_RULESNUM            INTEGER,
    LANGUAGE_USED              INTEGER,
    FK_DCD_RULEID              DECIMAL(12),
    B_COLOUR                   DECIMAL(12),
    FK_HPRODUCTVALIDITY_DATE   DATE,
    TMSTAMP                    TIMESTAMP(6),
    DRAW_TYPE_FLAG             CHAR(1),
    DESCRIPTION                CHAR(40),
    constraint IXU_DEF_004
        primary key (FK_DCD_PRODUCTID_P, FK_DCD_TRANSACTGUI, FK_DCD_JUSTIFICID, OTHER_GUI_TYPE, OTHER_GUI_VALUE)
);


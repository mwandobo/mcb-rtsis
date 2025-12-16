create table DCD_GH_WNDW_TRANS
(
    ID_JUSTIFIC     INTEGER  not null,
    ID_PRODUCT      INTEGER  not null,
    ID_TRANSACT     INTEGER  not null,
    LANGUAGE_USED   INTEGER  not null,
    OTHER_GUI_TYPE  SMALLINT not null,
    OTHER_GUI_VALUE CHAR(5)  not null,
    VALIDITY_DATE   DATE     not null,
    DESCRIPTION     CHAR(40),
    constraint IXU_DEF_050
        primary key (ID_JUSTIFIC, ID_PRODUCT, ID_TRANSACT, LANGUAGE_USED, OTHER_GUI_TYPE, OTHER_GUI_VALUE,
                     VALIDITY_DATE)
);


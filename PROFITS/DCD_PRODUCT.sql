create table DCD_PRODUCT
(
    ID_PRODUCT       INTEGER not null
        constraint PKGUI016
            primary key,
    ENTRY_STATUS     CHAR(1),
    TMSTAMP          TIMESTAMP(6),
    PROD_DESCRIPTION VARCHAR(40)
);


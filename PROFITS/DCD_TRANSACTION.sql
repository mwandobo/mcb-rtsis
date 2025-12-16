create table DCD_TRANSACTION
(
    GUI_TRX_ID      INTEGER not null
        constraint PKGUI018
            primary key,
    SHORT_DESCR     CHAR(5),
    PRFT_SYSTEM     INTEGER,
    ENTRY_STATUS    CHAR(1),
    TMSTAMP         TIMESTAMP(6),
    GUI_DESCRIPTION VARCHAR(80)
);


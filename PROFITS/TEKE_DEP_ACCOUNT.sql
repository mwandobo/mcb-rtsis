create table TEKE_DEP_ACCOUNT
(
    FK_CUSTOMERCUST_ID      INTEGER,
    IBAN                    VARCHAR(40),
    BOOK_BALANCE            DECIMAL(15, 2),
    CR_DB_INTEREST          DECIMAL(15, 2),
    BALANCE                 DECIMAL(15, 2),
    SHORT_DESCR             CHAR(5),
    FIXING                  DECIMAL(12, 6),
    EURO_BOOK_BAL           DECIMAL(15, 2),
    COBENEF_COUNT           SMALLINT,
    EURO_AMOUNT_PORTION     DECIMAL(15, 2),
    SUM_EURO_AMOUNT_PORTION DECIMAL(15, 2),
    BLOCKED_AMOUNT          DECIMAL(15, 2),
    SUM_EURO_BLOCKED_AMOUNT DECIMAL(15, 2)
);


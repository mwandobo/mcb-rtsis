create table GLG_D_REP_TABLES
(
    FK_GLG_H_REP_TATAB CHAR(2),
    SNUM               SMALLINT,
    ID_CURRENCY        INTEGER,
    STATEMENT_NUM      INTEGER,
    EURO_CONV          CHAR(1),
    ACCOUNT_ID         CHAR(21)
);

create unique index IXU_GLG_061
    on GLG_D_REP_TABLES (FK_GLG_H_REP_TATAB, SNUM);


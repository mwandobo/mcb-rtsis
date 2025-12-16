create table GLG_FINAL_TRNR
(
    FK_USRCODE         CHAR(8)  not null,
    FK_UNITCODE        INTEGER  not null,
    LINE_NUM           SMALLINT not null,
    TRN_SNUM           INTEGER  not null,
    TRN_DATE           DATE     not null,
    FK1UNITCODE        INTEGER,
    DOC_NUM            INTEGER,
    AMOUNT             DECIMAL(15, 2),
    VALEUR_DATE        DATE,
    GL_TRN_DATE        DATE,
    DOC_TYPE           CHAR(1),
    ENTRY_TYPE         CHAR(1),
    FK_GLG_JOURNALJOUR CHAR(2),
    SUBSYSTEM          CHAR(2),
    FK_GLG_DOCUMENTDO0 CHAR(2),
    FK_GLG_JUSTIFYJUST CHAR(4),
    FK_GLG_DOCUMENTDOC CHAR(4),
    CURR_SHORT_DESCR   CHAR(5),
    FK_GLG_ACCOUNTACCO CHAR(21),
    REMARKS            VARCHAR(80),
    GLG_RULE           VARCHAR(2000),
    constraint IXU_GL_032
        primary key (FK_USRCODE, FK_UNITCODE, LINE_NUM, TRN_SNUM, TRN_DATE)
);


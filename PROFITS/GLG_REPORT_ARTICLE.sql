create table GLG_REPORT_ARTICLE
(
    TRN_ID            CHAR(6) not null
        constraint IXU_GLG_015
            primary key,
    FK_GD_REPORT_TYPE INTEGER,
    TMSTAMP           DATE,
    ENTRY_STATUS      CHAR(1),
    FK_GH_REPORT_TYPE CHAR(5),
    DESCRIPTION       VARCHAR(40)
);


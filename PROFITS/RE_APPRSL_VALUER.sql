create table RE_APPRSL_VALUER
(
    FK_APPRSL_SN     DECIMAL(10) not null,
    FK_REAL_ESTATEID DECIMAL(10) not null,
    FK_VALUER_ID     INTEGER     not null,
    ENTRY_STATUS     CHAR(1)
);

create unique index IXU_COL_040
    on RE_APPRSL_VALUER (FK_APPRSL_SN, FK_REAL_ESTATEID, FK_VALUER_ID);


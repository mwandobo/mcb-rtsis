create table XPAT_PSTEP
(
    ID              DECIMAL(10) not null
        constraint PATXPSPK
            primary key,
    VC_MODEL_ID     DECIMAL(10),
    PSTEP_NAME      CHAR(35)    not null,
    VC_PSTEP_SOURCE CHAR(8),
    LMW             CHAR(8),
    LMC             CHAR(8),
    CD_TRANCODE     CHAR(8),
    WD_TRANCODE     CHAR(8),
    STATUS          CHAR(1),
    MODEL_NAME      CHAR(32),
    LAST_CHANGED    TIMESTAMP(6),
    FK_PAT_PSTEPSID DECIMAL(10)
);

create unique index PATXPSI1
    on XPAT_PSTEP (FK_PAT_PSTEPSID);


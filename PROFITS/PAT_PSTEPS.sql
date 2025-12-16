create table PAT_PSTEPS
(
    ID              DECIMAL(10)  not null
        constraint PATPSPK1
            primary key,
    VC_MODEL_ID     DECIMAL(10)  not null,
    PSTEP_NAME      CHAR(35)     not null,
    VC_PSTEP_SOURCE CHAR(8)      not null,
    LMW             CHAR(8),
    LMC             CHAR(8),
    CD_TRANCODE     CHAR(8),
    WD_TRANCODE     CHAR(8),
    STATUS          CHAR(1)      not null,
    MODEL_NAME      CHAR(32)     not null,
    LAST_CHANGED    TIMESTAMP(6) not null
);


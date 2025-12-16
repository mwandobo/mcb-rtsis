create table COLL_ISSUER
(
    ISSUER_CODE                 INTEGER        not null
        constraint I0020808
            primary key,
    ISSUER_CD                   SMALLINT       not null,
    ISSUER_TYPE                 CHAR(1),
    NAME                        CHAR(20),
    SURNAME                     CHAR(70),
    ADDRESS                     CHAR(40)       not null,
    TELEPHONE                   VARCHAR(15),
    ID_NO                       CHAR(20),
    AFM_NO                      CHAR(20),
    PROFESSION                  CHAR(40),
    ISSUER_LIMIT                DECIMAL(15, 2) not null,
    APPROVAL_NO                 CHAR(30),
    ENTRY_STATUS                CHAR(1),
    TOTAL_CHEQUES               DECIMAL(15),
    FATHER_NAME                 VARCHAR(20),
    ISSUER_SPREAD_PERCENT_LIMIT DECIMAL(8, 4)  not null,
    ISSUER_SPREAD_LIMIT_TYPE    SMALLINT       not null
);


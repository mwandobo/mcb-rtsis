create table LG_BENEFICIARY
(
    CODE         INTEGER,
    FK_PROF_SN   INTEGER,
    TYPE         CHAR(1),
    ENTRY_STATUS CHAR(1),
    FK_PROF_TYPE CHAR(5),
    ZIP_CODE     CHAR(10),
    TELEPHONE    CHAR(15),
    AFM_NO       CHAR(20),
    NAME         CHAR(20),
    ID_NO        CHAR(20),
    CITY         CHAR(30),
    PROFESSION   CHAR(40),
    COMMENTS     CHAR(40),
    ADDRESS      CHAR(40),
    SURNAME      CHAR(70),
    REGION       VARCHAR(20),
    ADDRESS_2    VARCHAR(40)
);

create unique index IXU_LG__017
    on LG_BENEFICIARY (CODE);


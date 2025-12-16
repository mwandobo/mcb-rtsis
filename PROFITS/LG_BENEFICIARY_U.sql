create table LG_BENEFICIARY_U
(
    FK_PROF_TYPE CHAR(5),
    FK_PROF_SN   INTEGER,
    CODE         INTEGER not null
        constraint IXU_CIU_046
            primary key,
    TYPE         CHAR(1),
    NAME         CHAR(20),
    SURNAME      CHAR(70),
    ADDRESS      CHAR(40),
    ADDRESS_2    VARCHAR(40),
    REGION       VARCHAR(20),
    ZIP_CODE     CHAR(10),
    CITY         CHAR(30),
    TELEPHONE    CHAR(15),
    ID_NO        CHAR(20),
    AFM_NO       CHAR(20),
    ENTRY_STATUS CHAR(1),
    PROFESSION   CHAR(40),
    COMMENTS     CHAR(40)
);


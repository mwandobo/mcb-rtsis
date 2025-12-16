create table GENERIC_DET_TEMP
(
    FK_GENERIC_HEADPAR CHAR(5) not null,
    SERIAL_NUM         INTEGER not null,
    PARAMETER_TYPE     CHAR(5),
    SHORT_DESCRIPTION  CHAR(10),
    TMSTAMP            DATE,
    ENTRY_STATUS       CHAR(1),
    LATIN_DESC         VARCHAR(40),
    DESCRIPTION        VARCHAR(40)
);


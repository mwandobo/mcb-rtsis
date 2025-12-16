create table CAPTURED_TABLE
(
    NUMBER1    SMALLINT not null
        constraint I0000006
            primary key,
    NUMBER2    DECIMAL(15),
    NUMBERD3   DECIMAL(15, 2),
    CHAR1      CHAR(20),
    CHARVAR1   VARCHAR(20),
    DATE1      DATE,
    TMSTMP     TIMESTAMP(6),
    RADIO1     CHAR(1),
    RADIO2     CHAR(1),
    DROPLIST1  CHAR(1),
    DROPLIST2  SMALLINT,
    CHCKBOX1   CHAR(1),
    CHCKBOX2   CHAR(1),
    DBCHAR1    CHAR(15),
    DBNUMBERD1 DECIMAL(15, 2),
    DBDATE1    DATE,
    DBRADIO1   CHAR(1),
    DBDROP1    CHAR(1),
    DBCHKBOX1  CHAR(1)
);


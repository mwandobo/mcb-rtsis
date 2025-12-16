create table BDG_MODEL_TRN
(
    ID          CHAR(4) not null
        constraint IXU_GL_054
            primary key,
    STATUS      SMALLINT,
    TMSTMP      DATE,
    TYPE        CHAR(1),
    ELMNT_ID_1  CHAR(30),
    ELMNT_ID_3  CHAR(30),
    ELMNT_ID_4  CHAR(30),
    ELMNT_ID_5  CHAR(30),
    ELMNT_ID_6  CHAR(30),
    ELMNT_ID_7  CHAR(30),
    ELMNT_ID_8  CHAR(30),
    ELMNT_ID_9  CHAR(30),
    ELMNT_ID_10 CHAR(30),
    ELMNT_ID_11 CHAR(30),
    ELMNT_ID_12 CHAR(30),
    ELMNT_ID_13 CHAR(30),
    ELMNT_ID_14 CHAR(30),
    ELMNT_ID_15 CHAR(30),
    ELMNT_ID_16 CHAR(30),
    ELMNT_ID_17 CHAR(30),
    ELMNT_ID_18 CHAR(30),
    ELMNT_ID_19 CHAR(30),
    ELMNT_ID_20 CHAR(30),
    ELMNT_ID_2  CHAR(30),
    DESCRIPTION VARCHAR(40)
);


create table COMPASS
(
    DET_ID      DECIMAL(11) not null,
    CARD_TYPE   INTEGER     not null,
    LATFIO      CHAR(50),
    INN         CHAR(20),
    PASNOM      CHAR(20),
    COMPANY     CHAR(25)    not null,
    COMPANU2    CHAR(25)    not null,
    CARD_STATUS CHAR(1)     not null,
    SING_STATUS CHAR(1)     not null,
    BIRTHDAY    DATE,
    PAN         CHAR(16)    not null,
    ENT_DATE    DATE        not null,
    REC_ID      DECIMAL(11) not null
        constraint PRK
            primary key,
    FNPROF      SMALLINT    not null,
    C_DIGIT     SMALLINT    not null,
    CUST_ID     INTEGER     not null
);


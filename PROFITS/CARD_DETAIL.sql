create table CARD_DETAIL
(
    DET_ID             DECIMAL(11) not null
        constraint CMS5
            primary key,
    STATUS             CHAR(1)     not null,
    FIN_PROFIL         SMALLINT    not null,
    COMPANY_CODE       INTEGER     not null,
    PERIOD
    SMALLINT
    not null,
    ADDITIONAL         CHAR(1)     not null,
    PRIORITY_PASS      CHAR(1)     not null,
    PRIORITY_TRAVELLER CHAR(1)     not null,
    RESERV1            CHAR(10),
    RESERV2            CHAR(10),
    RESERV3            CHAR(10),
    RESERV4            CHAR(10),
    RESERV5            CHAR(10),
    RESERV6            CHAR(10),
    RESERV7            CHAR(10),
    RESERV8            CHAR(10),
    RESERV9            CHAR(10),
    RESERV10           CHAR(10),
    TMPSTM             TIMESTAMP(6),
    FK_CARD_ID         DECIMAL(11),
    FK_CARD_BIN_ID     DECIMAL(11)
);

create unique index I0000989
    on CARD_DETAIL (FK_CARD_ID);

create unique index I0000995
    on CARD_DETAIL (FK_CARD_BIN_ID);


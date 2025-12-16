create table CARD_DISAIN
(
    STATUS               CHAR(1)  not null,
    REC_ID               CHAR(11) not null
        constraint CMS6
            primary key,
    FK_CARD_DETAILDET_ID DECIMAL(11),
    FK_DISAINREC_ID      DECIMAL(11)
);

create unique index I0000985
    on CARD_DISAIN (FK_CARD_DETAILDET_ID);

create unique index I0011041
    on CARD_DISAIN (FK_DISAINREC_ID);


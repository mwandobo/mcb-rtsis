create table CARD_PRODUCT
(
    TMPSTAMP         TIMESTAMP(6),
    STATUS           CHAR(1)     not null,
    REC_ID           DECIMAL(11) not null
        constraint CMS7
            primary key,
    PRODUCT_NUMBER   INTEGER     not null,
    FK_CARD_DET_ID   DECIMAL(11),
    FK_DISAIN_REC_ID DECIMAL(11)
);

create unique index I0000983
    on CARD_PRODUCT (FK_CARD_DET_ID);

create unique index I0011043
    on CARD_PRODUCT (FK_DISAIN_REC_ID);


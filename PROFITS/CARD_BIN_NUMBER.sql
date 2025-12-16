create table CARD_BIN_NUMBER
(
    STATUS  CHAR(1)     not null,
    TMSTP   TIMESTAMP(6),
    BNUMBER VARCHAR(10) not null,
    B_ID    DECIMAL(11) not null
        constraint CMS3
            primary key
);


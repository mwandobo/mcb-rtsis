create table CARDS
(
    STATUS     CHAR(1)      not null,
    REC_ID     DECIMAL(11)  not null
        constraint CMS1
            primary key,
    SERIAL_NUM SMALLINT,
    CARD_NAME  VARCHAR(100) not null
);


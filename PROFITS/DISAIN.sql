create table DISAIN
(
    STATUS            CHAR(1)     not null,
    REC_ID            DECIMAL(11) not null
        constraint CMS10
            primary key,
    DESCRIPTION       CHAR(40),
    SHORT_DESCRIPTION CHAR(20),
    SERIAL_NUM        INTEGER     not null
);


create table CIE_HISTORY_PASSWD
(
    CIE_CUSTOMER INTEGER not null,
    SERIAL_NUM   INTEGER not null,
    PASSWORD     CHAR(88),
    constraint IXU_DEF_098
        primary key (CIE_CUSTOMER, SERIAL_NUM)
);


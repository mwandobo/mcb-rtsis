create table COMM_POST_PAR_HDR
(
    SERIAL_NUM   INTEGER,
    ENTRY_STATUS CHAR(1),
    DESCR        CHAR(50)
);

create unique index IXU_COM_004
    on COMM_POST_PAR_HDR (SERIAL_NUM);


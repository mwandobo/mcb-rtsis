create table PACK
(
    ID_PACKAGE         INTEGER not null
        constraint IDPACKAG
            primary key,
    INSERTION_DT       DATE,
    PACKAGE_DESCR      CHAR(80),
    MODIFICATION_DT    DATE,
    MODIFICATION_USER  CHAR(8),
    PACKAGE_STATUS_FLG CHAR(1),
    PACKET_FUNCTION    CHAR(250)
);


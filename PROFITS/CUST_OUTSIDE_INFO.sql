create table CUST_OUTSIDE_INFO
(
    FK_GENERIC_DETAFK  CHAR(5),
    FK_GENERIC_DETASER INTEGER,
    EXTRACT_DATE       DATE,
    HEADER_4           CHAR(10),
    HEADER_5           CHAR(10),
    HEADER_3           CHAR(10),
    HEADER_6           CHAR(10),
    HEADER_1           CHAR(10),
    HEADER_2           CHAR(10)
);

create unique index IXU_CUS_013
    on CUST_OUTSIDE_INFO (FK_GENERIC_DETAFK, FK_GENERIC_DETASER, EXTRACT_DATE);


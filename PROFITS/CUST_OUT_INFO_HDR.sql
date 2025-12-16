create table CUST_OUT_INFO_HDR
(
    HEADER_TYPE SMALLINT,
    HEADER_2    CHAR(17),
    HEADER_6    CHAR(17),
    HEADER_4    CHAR(17),
    HEADER_5    CHAR(17),
    HEADER_1    CHAR(17),
    HEADER_3    CHAR(17)
);

create unique index IXU_CUS_012
    on CUST_OUT_INFO_HDR (HEADER_TYPE);


create table CUST_OUTSIDE_INFO0
(
    FK_CUSTOMERCUST_ID INTEGER  not null,
    FK_CUST_OUTSIDEFK  CHAR(5)  not null,
    FK0CUST_OUTSIDEFK  INTEGER  not null,
    FK_CUST_OUTSIDEEXT DATE     not null,
    OUT_ACCOUNT        CHAR(16) not null,
    VALUE_1            CHAR(3),
    VALUE_6            CHAR(5),
    VALUE_3            CHAR(15),
    VALUE_4            CHAR(15),
    VALUE_2            CHAR(16),
    VALUE_5            CHAR(40),
    constraint IXU_CIS_185
        primary key (FK_CUSTOMERCUST_ID, FK_CUST_OUTSIDEFK, FK0CUST_OUTSIDEFK, FK_CUST_OUTSIDEEXT, OUT_ACCOUNT)
);


create table CUST_ADDR_PARREL_H
(
    CODE         CHAR(8) not null
        constraint IXU_PARRELAT_H
            primary key,
    P00_CODEDESC VARCHAR(80),
    P01_HDESC    VARCHAR(40),
    P02_HDESC    VARCHAR(40),
    P03_HDESC    VARCHAR(40),
    P04_HDESC    VARCHAR(40),
    P05_HDESC    VARCHAR(40),
    P06_HDESC    VARCHAR(40),
    P07_HDESC    VARCHAR(40),
    P08_HDESC    VARCHAR(40),
    P09_HDESC    VARCHAR(40),
    P10_HDESC    VARCHAR(40)
);


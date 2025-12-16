create table CUST_ADDR_PARREL_D
(
    CODE     CHAR(8) not null,
    SN       INTEGER not null,
    P01_DESC VARCHAR(40),
    P02_DESC VARCHAR(40),
    P03_DESC VARCHAR(40),
    P04_DESC VARCHAR(40),
    P05_DESC VARCHAR(40),
    P06_DESC VARCHAR(40),
    P07_DESC VARCHAR(40) default ' ',
    P08_DESC VARCHAR(40) default ' ',
    P09_DESC VARCHAR(40) default ' ',
    P10_DESC VARCHAR(40) default ' ',
    constraint IXU_PARRELATION_D
        primary key (CODE, SN)
);


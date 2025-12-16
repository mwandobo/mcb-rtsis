create table SO_CUST_COMMISS_U
(
    NEXT_ACTIV_DATE         DATE,
    JUSTIFIC_ID             INTEGER  not null,
    CUST_ID                 INTEGER  not null
        constraint IXU_CIU_054
            primary key,
    CUST_CD                 SMALLINT,
    COUNT_OF_SO             INTEGER  not null,
    FREQUENCY_UNITOF_MEAS   CHAR(1)  not null,
    FREQUENCY               SMALLINT not null,
    LAST_COMMISS_DATE       DATE     not null,
    LAST_COMMISS_HAS_FAILED CHAR(1)
);


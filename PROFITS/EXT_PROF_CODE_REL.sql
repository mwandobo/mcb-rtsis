create table EXT_PROF_CODE_REL
(
    EXT_CODE CHAR(30) not null
        constraint I0001206
            primary key,
    CUST_ID  INTEGER,
    CUST_CD  SMALLINT
);


create table LNS_CUST_RECLASS
(
    CUST_ID     INTEGER not null
        constraint I0000612
            primary key,
    TRX_DATE    DATE,
    PROCESS_FLG CHAR(1)
);


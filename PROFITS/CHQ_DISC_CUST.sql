create table CHQ_DISC_CUST
(
    CODE           INTEGER  not null,
    TRN_BANK       SMALLINT not null,
    TMSTAMP        DATE,
    BOUNCED_STATUS CHAR(1),
    USR            CHAR(8),
    FIRST_NAME     CHAR(20),
    COMMENTS       CHAR(60),
    LAST_NAME      CHAR(60),
    constraint IXU_DEP_169
        primary key (CODE, TRN_BANK)
);


create table CMS_CLEARING_LOG
(
    HEADER     CHAR(20) not null
        constraint PK_CLEARING_FILE
            primary key,
    LOADING_DT DATE,
    TMSTAMP    TIMESTAMP(6)
);


create table SQL_BATCH_DETAIL
(
    DETAIL_ID   DECIMAL(15) not null,
    SN          DECIMAL(15) not null,
    PROGRAM_ID  CHAR(5)     not null,
    DESCRIPTION VARCHAR(100),
    constraint PK_SQL_DETAIL
        primary key (PROGRAM_ID, SN, DETAIL_ID)
);


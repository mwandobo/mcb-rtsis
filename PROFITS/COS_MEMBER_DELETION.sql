create table COS_MEMBER_DELETION
(
    MEMBER_ID         DECIMAL(10) not null,
    TRX_DATE          DATE        not null,
    ACCOUNT_NUMBER    CHAR(40),
    NUMBER_OF_SHARES  DECIMAL(15),
    SELECT_ALL_SHARES CHAR(1),
    PROCESS_STATUS    SMALLINT,
    ERROR_DESC        CHAR(80),
    constraint PK_MEMB_DELET
        primary key (MEMBER_ID, TRX_DATE)
);


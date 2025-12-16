create table USER_COUNTER
(
    USER_CODE  CHAR(8) not null,
    GROUP_CODE CHAR(8) not null,
    CNTR       DECIMAL(12),
    TMSTAMP    TIMESTAMP(6),
    constraint IXU_USR_CNTR1
        primary key (USER_CODE, GROUP_CODE)
);


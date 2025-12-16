create table PROFITS_EXIT_CNTR
(
    PRFT_SYSTEM SMALLINT not null,
    LANGUAGE0   INTEGER  not null,
    CNTR        DECIMAL(12),
    constraint PKEXSTCN
        primary key (LANGUAGE0, PRFT_SYSTEM)
);


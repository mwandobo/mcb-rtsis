create table BENCHMARK_LOCK_USR
(
    CODE         CHAR(8) not null
        constraint IXU_BNC_004
            primary key,
    ENTRY_STATUS CHAR(1)
);


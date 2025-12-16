create table MIG_GL_CROSS_CHECK
(
    SERIAL_NO    INTEGER  not null
        constraint MIG_GL_CROSS_CHECK_PK
            primary key,
    UNIT         INTEGER  not null,
    MIGR_BALANCE DECIMAL(15, 2),
    GL_BALANCE   DECIMAL(15, 2),
    GL_ACCOUNT   CHAR(21) not null
);


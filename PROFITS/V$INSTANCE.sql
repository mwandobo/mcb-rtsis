create table V$INSTANCE
(
    HOST_NAME     VARCHAR(15),
    INSTANCE_NAME VARCHAR(15) not null
        constraint I0000788
            primary key
);


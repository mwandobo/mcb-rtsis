create table BLK_FILE_COUNTER
(
    FILE_COUNTER DECIMAL(10) generated always as identity
        primary key,
    TIMESTAMP    TIMESTAMP(6)
);


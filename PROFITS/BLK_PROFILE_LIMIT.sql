create table BLK_PROFILE_LIMIT
(
    MIN_AMOUNT DECIMAL(15, 2),
    MAX_AMOUNT DECIMAL(15, 2),
    FK_PROFILE CHAR(8) not null
        constraint PK_BLK_PROF_LIMT
            primary key
);


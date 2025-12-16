create table GLG_PROFILE_LIMIT
(
    MIN_AMOUNT DECIMAL(18, 2),
    MAX_AMOUNT DECIMAL(18, 2),
    FK_PROFILE CHAR(8)                 not null,
    SCREEN_ID  CHAR(5) default '05290' not null,
    primary key (FK_PROFILE, SCREEN_ID)
);


create table WFS_ROLE_LIMITS
(
    FK_ROLE_ID     DECIMAL(10) not null,
    FK_CURRENCYID  INTEGER     not null,
    LIMIT_AMN      DECIMAL(15, 2),
    LIMIT_COMMENTS VARCHAR(2048),
    WFS_LIMIT_STS  CHAR(1),
    constraint PK_ROLE_LIMIT
        primary key (FK_CURRENCYID, FK_ROLE_ID)
);


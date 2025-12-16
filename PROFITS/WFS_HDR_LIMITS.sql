create table WFS_HDR_LIMITS
(
    LIMIT_AMN      DECIMAL(15, 2),
    FK_CURRENCYID  INTEGER     not null,
    FK_WF_HEADER   DECIMAL(10) not null,
    LIMIT_COMMENTS VARCHAR(500),
    constraint PH_HDR_LIMIT
        primary key (FK_CURRENCYID, FK_WF_HEADER)
);


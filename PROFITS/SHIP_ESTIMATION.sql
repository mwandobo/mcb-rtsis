create table SHIP_ESTIMATION
(
    INTERNAL_SN      DECIMAL(10) not null,
    ESTIMATOR        CHAR(40),
    ESTIMATION_DT    DATE,
    ESTIMATED_VALUE  DECIMAL(15, 2),
    ESTIMATION_CMNTS CHAR(254),
    COMMENTS         CHAR(254),
    ENTRY_STATUS     CHAR(1),
    FK_SHIPID        DECIMAL(10) not null,
    constraint PK_SHIPAPR
        primary key (FK_SHIPID, INTERNAL_SN)
);


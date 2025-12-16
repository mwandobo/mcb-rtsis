create table CORRESPONDENT_CUT
(
    CUTOFF_TIME      DATE    not null,
    CUST_ID          INTEGER not null,
    DESCRIPTION      CHAR(40),
    FK_GH_SERVICE    CHAR(5) not null,
    FK_GD_SERVICE    INTEGER not null,
    MIN_VALUE_SPREAD SMALLINT,
    MAX_VALUE_SPREAD SMALLINT,
    constraint PK_CORR_CUTOFF
        primary key (FK_GH_SERVICE, FK_GD_SERVICE, CUST_ID)
);


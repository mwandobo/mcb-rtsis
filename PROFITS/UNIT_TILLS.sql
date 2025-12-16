create table UNIT_TILLS
(
    TILL_NO     INTEGER not null,
    TMSTMP      DATE,
    STATUS      CHAR(1),
    FK_UNITCODE INTEGER not null,
    constraint PK_UNIT_TILLS
        primary key (FK_UNITCODE, TILL_NO)
);


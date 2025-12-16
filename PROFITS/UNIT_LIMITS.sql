create table UNIT_LIMITS
(
    BRANCH_LIMIT   DECIMAL(15, 2),
    ATMS_LIMIT     DECIMAL(15, 2),
    LIMIT_COMMENTS VARCHAR(200),
    FK_CURRENCY    INTEGER not null,
    FK_UNITCODE    INTEGER not null,
    ENTRY_STATUS   CHAR(1),
    constraint PK_UNITLMT
        primary key (FK_CURRENCY, FK_UNITCODE)
);


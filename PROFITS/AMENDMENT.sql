create table AMENDMENT
(
    FK_AGREEMENTAGREEM DECIMAL(10) not null,
    SERIAL_NUMBER      SMALLINT    not null,
    ISSUE_DATE         DATE,
    AMOUNT             DECIMAL(15, 2),
    AGREEMENT_TYPE     CHAR(2),
    APPROVAL_DATE      DATE,
    APPROVAL_NUMBER    DECIMAL(12),
    ENTRY_COMMENTS     CHAR(40),
    constraint PK_AMENDMENT
        primary key (FK_AGREEMENTAGREEM, SERIAL_NUMBER)
);


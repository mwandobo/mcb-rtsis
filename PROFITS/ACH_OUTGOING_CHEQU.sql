create table ACH_OUTGOING_CHEQU
(
    FK_COLLABORATIOBAN INTEGER     not null,
    CHEQUE_NUMBER      CHAR(20)    not null,
    ISSUE_DATE         DATE        not null,
    IDENTIFIER         DECIMAL(13) not null,
    CHEQUE_AMOUNT      DECIMAL(15, 2),
    constraint IXU_DEP_112
        primary key (FK_COLLABORATIOBAN, CHEQUE_NUMBER, ISSUE_DATE, IDENTIFIER)
);


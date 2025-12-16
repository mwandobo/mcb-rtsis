create table COMMITMENT_PAY_DAYS
(
    TP_SO_IDENTIFIER DECIMAL(10) not null,
    PAY_DAY          SMALLINT    not null,
    constraint IXU_COM_002
        primary key (TP_SO_IDENTIFIER, PAY_DAY)
);


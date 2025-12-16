create table AMEND_PSB
(
    ACCOUNT_NUMBER DECIMAL(11) not null
        constraint IXU_DEF_109
            primary key,
    AMEND_STATUS   CHAR(1)
);


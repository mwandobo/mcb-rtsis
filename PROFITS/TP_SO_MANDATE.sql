create table TP_SO_MANDATE
(
    TP_SO_IDENTIFIER     DECIMAL(10) not null
        constraint TPSOMAND
            primary key,
    ORIGINATOR_REFERENCE CHAR(4),
    DB_ACCOUNT           CHAR(15),
    DB_BANK              CHAR(2),
    DB_BRANCH            CHAR(3),
    POLICY_NUMBER_1      CHAR(20),
    POLICY_NUMBER_2      CHAR(20),
    REMARKS              CHAR(25)
);


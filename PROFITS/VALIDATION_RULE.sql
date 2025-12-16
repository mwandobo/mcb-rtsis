create table VALIDATION_RULE
(
    PRFT_SYSTEM      SMALLINT    not null,
    ID               DECIMAL(12) not null,
    SNUM             INTEGER     not null,
    DESCRIPTION      CHAR(50),
    LINE_DESCRIPTION CHAR(60),
    CALCULATION      CHAR(30),
    FIRST_FIELD      CHAR(30),
    FUNCTION         CHAR(2),
    SECOND_FIELD     CHAR(30),
    PROFITS_EXIT_ST  DECIMAL(10),
    GO_TO_LINE       SMALLINT,
    CHECK_FIELD_FLG  CHAR(2),
    DATE_ADD         CHAR(2),
    ENTIRE_LINE_FLG  CHAR(2),
    constraint PK_VRULE
        primary key (ID, SNUM, PRFT_SYSTEM)
);


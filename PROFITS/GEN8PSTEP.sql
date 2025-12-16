create table GEN8PSTEP
(
    ENCY       INTEGER     not null,
    MODEL      INTEGER     not null,
    SOURCE     VARCHAR(8)  not null,
    PSTEP_NAME VARCHAR(35) not null,
    LMW        VARCHAR(8),
    LMC        VARCHAR(8),
    LMO        VARCHAR(8),
    WC         VARCHAR(8),
    WD         VARCHAR(8),
    CD         VARCHAR(8),
    CC         VARCHAR(8),
    TYPE0      VARCHAR(1),
    constraint PK_GEN8PSTEPS
        primary key (SOURCE, MODEL, ENCY)
);


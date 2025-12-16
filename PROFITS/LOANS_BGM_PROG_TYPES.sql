create table LOANS_BGM_PROG_TYPES
(
    BGM_PROGRAM      VARCHAR(20) not null,
    BGM_TYPE         INTEGER     not null,
    BGM_PROGRAM_STEP INTEGER     not null,
    BGM_MANDATORY    CHAR(1),
    BGM_MULTIPLE     CHAR(1),
    TMSTAMP          TIMESTAMP(6),
    ENTRY_STATUS     CHAR(1),
    BGM_COMMENTS     VARCHAR(100),
    BGM_HAS_DETAILS  CHAR(1),
    constraint BGMSTEP
        primary key (BGM_PROGRAM, BGM_TYPE, BGM_PROGRAM_STEP)
);


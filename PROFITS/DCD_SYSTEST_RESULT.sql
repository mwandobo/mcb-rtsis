create table DCD_SYSTEST_RESULT
(
    PRFT_SYSTEM       SMALLINT    not null,
    RESULT_SN         DECIMAL(15) not null,
    RUN_DATE          DATE        not null,
    TESTCASE_SN       DECIMAL(10) not null,
    VALRULE_ID        DECIMAL(12) not null,
    ERROR_LINE        DECIMAL(10),
    PRV_EXIT_STATE    DECIMAL(12),
    CURR_EXIT_STATE   DECIMAL(12),
    UNSUCCESFUL_FLG   CHAR(1),
    SUCCESFUL_FLG     CHAR(1),
    PROBLEM_ENTITY    CHAR(40),
    PROBLEM_ATTRIBUTE CHAR(40),
    CURR_ACTUAL_MSG   VARCHAR(2048),
    constraint IXU_DEF_121
        primary key (PRFT_SYSTEM, RESULT_SN, RUN_DATE, TESTCASE_SN, VALRULE_ID)
);


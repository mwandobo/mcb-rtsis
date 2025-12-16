create table MAND_OPT_WIN_CASES
(
    SEC_WIN_CODE CHAR(8)  not null,
    CASE_PARAM   CHAR(40) not null,
    CASE_VALUE   CHAR(40) not null,
    WINDOW_NAME  CHAR(50) not null,
    WINDOW_FIELD CHAR(40) not null,
    ALTER_PROMPT VARCHAR(60),
    constraint PK_MAND_CASE
        primary key (WINDOW_FIELD, WINDOW_NAME, CASE_VALUE, CASE_PARAM, SEC_WIN_CODE)
);


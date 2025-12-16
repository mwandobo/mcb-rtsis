create table PAT_RUNNING_STACK
(
    LEVEL0             INTEGER  not null,
    TYPE0              CHAR(1)  not null,
    HANDLE             CHAR(10),
    WINDOW_TITLE       CHAR(100),
    PSTEP_NAME         CHAR(32),
    DLL_NAME           CHAR(8),
    FK_USER_PROFILE_ID CHAR(10) not null,
    constraint PATSKPK1
        primary key (FK_USER_PROFILE_ID, LEVEL0, TYPE0)
);


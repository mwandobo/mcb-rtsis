create table WFS_ROLE_PROFILES
(
    WFS_ROLE_PROF_STS CHAR(1),
    FK_PROFILE        CHAR(8)     not null,
    FK_WORKFLOW_ROLE  DECIMAL(10) not null,
    constraint PK_WF_ROLEPROF
        primary key (FK_PROFILE, FK_WORKFLOW_ROLE)
);


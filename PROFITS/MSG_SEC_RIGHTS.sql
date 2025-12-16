create table MSG_SEC_RIGHTS
(
    PROFILE     VARCHAR(10)        not null,
    WINDOW_CODE VARCHAR(8)         not null,
    ACTION      VARCHAR(8)         not null,
    STATUS      SMALLINT default 1 not null
);

create unique index MSG_SEC_RIGHTS_PK
    on MSG_SEC_RIGHTS (PROFILE, WINDOW_CODE, ACTION);


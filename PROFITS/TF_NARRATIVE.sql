create table TF_NARRATIVE
(
    ACCOUNT_NUMBER CHAR(40) not null,
    TAG            CHAR(5)  not null,
    SN             INTEGER  not null,
    NARRATIVE      CHAR(250),
    constraint PK_TF_NARRATIVE
        primary key (SN, TAG, ACCOUNT_NUMBER)
);


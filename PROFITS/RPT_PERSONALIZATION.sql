create table RPT_PERSONALIZATION
(
    USER_CODE VARCHAR(50)  not null
        constraint RPT_PERSONALIZATION_PK
            primary key,
    SETTINGS  CLOB(102400) not null
);


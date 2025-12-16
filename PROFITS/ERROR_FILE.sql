create table ERROR_FILE
(
    LANGUAGE       SMALLINT    not null,
    ENG_LANG_TEXT  VARCHAR(80) not null,
    BANK_LANG_TEXT VARCHAR(80) not null,
    constraint IXU_SWI_100
        primary key (ENG_LANG_TEXT, LANGUAGE)
);


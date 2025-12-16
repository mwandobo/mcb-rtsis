create table VER_VERSIONS
(
    VERSION_DATE      VARCHAR(6)  not null,
    VERSION_NUMBER    VARCHAR(4)  not null,
    ENVIRONMENT_ID    SMALLINT    not null,
    USER_ID           SMALLINT    not null,
    MODEL_NAME_ID     SMALLINT,
    VERSION_TIMESTAMP DATE,
    VERSION_TYPE      VARCHAR(20) not null,
    CREATED_DATE      DATE        not null,
    TRANSFERED_DATE   DATE,
    RELEASE_NUMBER    VARCHAR(50),
    TARGET_RELEASE_ID DECIMAL(11),
    PATCH_NUMBER      INTEGER,
    VERSION_CODE      VARCHAR(12) not null,
    constraint VERSION_HEADER_PK
        primary key (VERSION_DATE, VERSION_NUMBER)
);

create unique index INDEX_VERSION_CODE
    on VER_VERSIONS (VERSION_CODE);


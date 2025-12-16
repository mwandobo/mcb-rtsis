create table VER_GEN_DETAILS
(
    VERSION_DATE      VARCHAR(6)  not null,
    VERSION_NUMBER    VARCHAR(4)  not null,
    MEMBER_NAME       VARCHAR(8)  not null,
    MEMBER_TYPE       VARCHAR(10) not null,
    GROUP_NUMBER      DECIMAL(10) not null,
    MEMBER_CHECKED    VARCHAR(1),
    SOURCE_TYPE       VARCHAR(10) not null,
    FILE_NAME         VARCHAR(20) not null,
    REVISION_DATETIME DATE        not null,
    VERSION_CODE      VARCHAR(12) not null,
    constraint GEN_DETAILS_PK
        primary key (VERSION_DATE, VERSION_NUMBER, MEMBER_NAME, FILE_NAME)
);


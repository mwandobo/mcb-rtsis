create table ASSET_CHARGES
(
    ASSET_ID        VARCHAR(10) not null,
    ROW_NUMBER      INTEGER     not null,
    CHARGED_DATE    DATE        not null,
    CHARGED_DEP     INTEGER,
    CHARGED_ADMIN   INTEGER,
    CHARGED_UNIT    INTEGER,
    RECEIVAL_DATE   DATE,
    CHARGED_USR     CHAR(8),
    ADDITIONAL_INFO VARCHAR(2048),
    constraint IXU_GL_050
        primary key (ASSET_ID, ROW_NUMBER, CHARGED_DATE)
);


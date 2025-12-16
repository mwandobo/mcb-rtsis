create table RPT_TEMPLATE
(
    ID                    INTEGER      not null
        constraint RPT_TEMPLATE_PK
            primary key,
    FK_REPORT_ID          INTEGER      not null,
    FK_LANGUAGE_ID        INTEGER      not null,
    FK_FILE_ID            INTEGER      not null,
    CREATED               TIMESTAMP(6) not null,
    CREATED_BY            VARCHAR(50)  not null,
    UPDATED               TIMESTAMP(6) not null,
    UPDATED_BY            VARCHAR(50)  not null,
    COMMENTS              VARCHAR(2000),
    FK_ATTACHMENT_FILE_ID INTEGER,
    FK_ORGANIZATION_ID    SMALLINT default 0
);


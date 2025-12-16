create table GL_EXTRACT_CRITERIA
(
    TRX_USER      CHAR(8) not null
        constraint PK_GL_EXTRCR
            primary key,
    SELECTED_DATE DATE,
    ID_CURRENCY   INTEGER,
    GLG_ACCOUNT   CHAR(21),
    LAST_REMARKS  CHAR(50)
);


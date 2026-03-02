create table INCOME_STATEMENT_GL_LOOKUP
(
    GL_ACCOUNT     CHAR(100)    null,
    CATEGORY     CHAR(100)     null,
    ITEM_CODE  CHAR(100)     null,
    DESCRIPTION  CHAR(100)     null,
 
    constraint SHARE_CAPITAL_PK
        primary key (SHAREHOLDER_NAME)
);

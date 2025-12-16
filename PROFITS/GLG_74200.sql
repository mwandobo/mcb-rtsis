create table GLG_74200
(
    PRODUCT_ID         INTEGER,
    ENTRY_TYPE         CHAR(1),
    NETWORK_GL_ACCOUNT CHAR(21),
    GL_ACCOUNT_NUMBER  CHAR(21)
);

create unique index I0000809
    on GLG_74200 (PRODUCT_ID, ENTRY_TYPE);


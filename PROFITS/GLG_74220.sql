create table GLG_74220
(
    NETWORK_GL_ACCOUNT VARCHAR(21) not null,
    GL_ACCOUNT_NUMBER  VARCHAR(21) not null,
    ENTRY_TYPE         VARCHAR(1)  not null,
    PRODUCT_ID         INTEGER     not null,
    constraint I0010806
        primary key (PRODUCT_ID, ENTRY_TYPE)
);


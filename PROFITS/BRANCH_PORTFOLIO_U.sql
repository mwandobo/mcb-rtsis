create table BRANCH_PORTFOLIO_U
(
    BRANCH         INTEGER not null,
    PORTFOLIO_CODE INTEGER not null,
    DESCRIPTION    CHAR(40),
    FK_USRCODE     CHAR(8),
    ENTRY_STATUS   CHAR(1),
    constraint IXU_CIU_005
        primary key (PORTFOLIO_CODE, BRANCH)
);


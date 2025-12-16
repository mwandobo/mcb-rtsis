create table SEC_ROLE
(
    CODE            INTEGER,
    FK_SEC_ROLECODE INTEGER,
    TMSTAMP         TIMESTAMP(6),
    ENTRY_STATUS    CHAR(1),
    PROOF_NEED      CHAR(1),
    DESCRIPTION     VARCHAR(50)
);

create unique index IXU_SEC_005
    on SEC_ROLE (CODE);


create table SDD_CREDITORS
(
    CODE      VARCHAR(35) not null
        constraint IX_SDD_CRED
            primary key,
    STATUS    CHAR(1),
    NAME      VARCHAR(70),
    ADDRESS_1 VARCHAR(70),
    ADDRESS_2 VARCHAR(70),
    COMMENTS  VARCHAR(70)
);

comment on table SDD_CREDITORS is 'Organizations info';

comment on column SDD_CREDITORS.CODE is 'Creditor Code';

comment on column SDD_CREDITORS.STATUS is '0: Creditor Active1: Creditor Inactive';

comment on column SDD_CREDITORS.NAME is 'Creditor Name';

comment on column SDD_CREDITORS.ADDRESS_1 is 'Creditor Address Line 1';

comment on column SDD_CREDITORS.ADDRESS_2 is 'Creditor Address Line 2';


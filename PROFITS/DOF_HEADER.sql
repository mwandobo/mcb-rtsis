create table DOF_HEADER
(
    START_DATE          DATE,
    SUSPEND_DATE        DATE,
    TOTAL_AMOUNT        DECIMAL(15, 2),
    LAST_PAYMENT_AMOUNT DECIMAL(15, 2),
    LAST_PAYMENT_DATE   DATE,
    INSERT_TIMESTAMP    TIMESTAMP(6),
    UPDATE_TIMESTAMP    TIMESTAMP(6),
    ENTRY_STATUS        CHAR(1),
    FK_CP_AGREEMENT     DECIMAL(10) not null,
    FK_CUSTOMER         DECIMAL(7)  not null,
    FK_UPD_USRCODE      CHAR(8),
    FK_INS_USRCODE      CHAR(8),
    constraint I0000951
        primary key (FK_CUSTOMER, FK_CP_AGREEMENT)
);

create unique index I0000948
    on DOF_HEADER (FK_CP_AGREEMENT);

create unique index I0000950
    on DOF_HEADER (FK_CUSTOMER);

create unique index I0000953
    on DOF_HEADER (FK_UPD_USRCODE);

create unique index I0000955
    on DOF_HEADER (FK_INS_USRCODE);


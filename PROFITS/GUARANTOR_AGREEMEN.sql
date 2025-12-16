create table GUARANTOR_AGREEMEN
(
    FK_AGREEMENTAGREEM DECIMAL(10)  not null,
    FK_CUSTOMERCUST_ID INTEGER      not null,
    ISSUE_DATE         DATE,
    TIMESTMP           TIMESTAMP(6) not null,
    COMMENTS0          CHAR(40),
    STATUS0            CHAR(1),
    constraint PK_GUARAN_AGREEMEN
        primary key (FK_CUSTOMERCUST_ID, FK_AGREEMENTAGREEM)
);


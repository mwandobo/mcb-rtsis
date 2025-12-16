create table GLG_POOLRATE_DETAIL
(
    TRAN_ID                CHAR(6) not null,
    FROM_DATE              DATE    not null,
    PERCENTAGE             DECIMAL(9, 6),
    BASEDAYS               CHAR(1),
    REMARKS                VARCHAR(100),
    LAST_CALCULATED_DATE   DATE,
    FK_GLG_POOLRATETRAN_ID CHAR(6)
        references GLG_POOLRATE_HEADER,
    constraint IXU_GLG_POOLRATE_D
        primary key (TRAN_ID, FROM_DATE)
);

create unique index I0010874
    on GLG_POOLRATE_DETAIL (FK_GLG_POOLRATETRAN_ID);


create table REPETITION
(
    LINE_NUMBER_ON_RUP SMALLINT     not null,
    RUPTURE_CRITERIA   CHAR(16)     not null,
    RUPTURE_CRITERIA_N SMALLINT     not null,
    FK_LINETIMESTAMP_I TIMESTAMP(6) not null,
    constraint I0000610
        primary key (FK_LINETIMESTAMP_I, LINE_NUMBER_ON_RUP)
);


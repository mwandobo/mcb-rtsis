create table ISS_DAMAGE_DTL
(
    TP_SO_IDENTIFIER DECIMAL(10) not null,
    TYPE_CODE        INTEGER     not null,
    YEAR0            SMALLINT,
    DAMAGE_AMNT      DECIMAL(15, 2),
    REPLACED         CHAR(1),
    constraint ISSDAMAG
        primary key (TYPE_CODE, TP_SO_IDENTIFIER)
);


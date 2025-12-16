create table CELL
(
    CELL_NUMBER        SMALLINT     not null,
    CELL_TYPE          CHAR(1)      not null,
    VARIABLE_NAME      CHAR(16)     not null,
    VARIABLE_NUMBER    SMALLINT     not null,
    CELL_TEXT          CHAR(32)     not null,
    WITH_BORDER        CHAR(1)      not null,
    ITALIC             CHAR(1)      not null,
    NUMBER_OF_CARACTER SMALLINT,
    JUSTIFICATION      CHAR(1)      not null,
    UNDERLINE          CHAR(2)      not null,
    BOLD_OR_NOT        CHAR(1)      not null,
    SPECIAL_TOTAL_ZONE CHAR(1)      not null,
    LEFT_ALIGNED_ON    SMALLINT     not null,
    RIGHT_ALIGNED_ON   SMALLINT     not null,
    FK_LINETIMESTAMP_I TIMESTAMP(6) not null,
    constraint I0000684
        primary key (FK_LINETIMESTAMP_I, CELL_NUMBER)
);


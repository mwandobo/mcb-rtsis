create table DCD_RT_STRUCT
(
    R_STRUCT_ID  CHAR(7) not null,
    STRUCT_ID    CHAR(7) not null,
    STRUCT_LEVEL SMALLINT,
    STRUCT_ORDER INTEGER,
    PRFT_SYSTEM  INTEGER,
    H_STRUCT_ID  CHAR(7),
    STRUCT_NAME  CHAR(60),
    constraint IXU_DEF_011
        primary key (R_STRUCT_ID, STRUCT_ID)
);


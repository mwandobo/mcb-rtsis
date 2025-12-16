create table FXFT_PERMITTED_STS
(
    PRIM_TYPE     CHAR(1)  not null,
    PRIM_CURR_STS CHAR(1)  not null,
    REVERSAL      CHAR(1)  not null,
    PRIM_NEW_STS  CHAR(1)  not null,
    SEC_TYPE      CHAR(1)  not null,
    SEC_CURR_STS  CHAR(1)  not null,
    SEC_NEW_STS   CHAR(1)  not null,
    SECKEY        CHAR(10) not null,
    constraint PK_FXFT_PER_STS
        primary key (PRIM_TYPE, PRIM_CURR_STS, REVERSAL)
);


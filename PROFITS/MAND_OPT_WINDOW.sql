create table MAND_OPT_WINDOW
(
    WIND_CODE           INTEGER     not null
        constraint IXU_DCD_046
            primary key,
    PSTEP_NAME          VARCHAR(50) not null,
    CLT_DLG_WIN_NAME    VARCHAR(50) not null,
    CLT_DLG_WIN_CAPTION VARCHAR(50) not null,
    SEC_WIN_CODE        CHAR(8)
);


create table DCD_JOIN_WNDW
(
    FK_PRFT_PROC_ID  INTEGER not null,
    FK_PRFT_TRANS_ID INTEGER not null,
    USE_STATUS       CHAR(1),
    STATUS           CHAR(1),
    DESCRIPTION      CHAR(40),
    constraint IXU_DEF_006
        primary key (FK_PRFT_PROC_ID, FK_PRFT_TRANS_ID)
);

